/*
 * Copyright © 2017 Simon Désaulniers
 * Author: Simon Désaulniers <sim.desaulniers@gmail.com>
 *
 * This file is part of dpaste.
 *
 * dpaste is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * dpaste is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with dpaste.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <algorithm>
#include <random>
#include <future>
#include <cstdlib>
#include <filesystem>
#include <iostream>

#include <opendht.h>
#include <glibmm.h>

#include "node.h"

namespace dpaste {

const constexpr char* Node::DPASTE_USER_TYPE;

namespace {

/**
 * Directory holding the on-disk OpenDHT node state.
 * Can be overridden with the DPASTE_CACHE_DIR environment variable
 * (e.g. for tests). Defaults to ${XDG_CACHE_HOME}/dpaste.
 */
std::string cacheDir() {
    const char* env = std::getenv("DPASTE_CACHE_DIR");
    std::string dir = env and *env ? env : Glib::get_user_cache_dir() + "/dpaste";
    std::error_code ec;
    std::filesystem::create_directories(dir, ec);
    return dir;
}

} /* anonymous namespace */

void Node::run(uint16_t port, std::string bootstrap_hostname, std::string bootstrap_port) {
    if (running_)
        return;

    auto dir = cacheDir();
    /* Ask OpenDHT to load its state (routing table) on start and save it on
     * shutdown; this reuses known peers and improves bootstrap resilience. */
    dht::DhtRunner::Config config;
    config.dht_config.node_config.persist_path = dir + "/nodes";
    config.threaded = true;
    node_.run(port, config);

    node_.bootstrap(bootstrap_hostname, bootstrap_port);
    running_ = true;
}

bool Node::paste(const std::string& code, dht::Blob&& blob, dht::DoneCallbackSimple&& cb) {
    auto v = std::make_shared<dht::Value>(std::forward<dht::Blob>(blob));
    v->user_type = DPASTE_USER_TYPE;

    auto hash = dht::InfoHash::get(code);

    if (cb) {
        node_.put(hash, v, cb);
        return true;
    } else {
        std::mutex mtx;
        std::condition_variable cv;
        std::unique_lock<std::mutex> lk(mtx);
        bool done = false, success_ {false};
        node_.put(hash, v, [&](bool success) {
            {
                std::unique_lock<std::mutex> lk(mtx);
                if (not success)
                    std::cerr << OPERATION_FAILURE_MSG << " (put)" << std::endl;
                else
                    success_ = true;
                done = true;
            }
            cv.notify_all();
        });
        cv.wait(lk, [&](){ return done; });
        return success_;
    }
}

void Node::get(const std::string& code, PastedCallback&& pcb) {
    auto blobs = std::make_shared<std::vector<dht::Blob>>();
    node_.get(dht::InfoHash::get(code),
        [blobs](std::shared_ptr<dht::Value> value) {
            blobs->emplace_back(value->data);
            return true;
        },
        [pcb,blobs](bool success) {
            if (not success)
                std::cerr << OPERATION_FAILURE_MSG << " (get)" << std::endl;
            else if (pcb)
                pcb(*blobs);
        }, dht::Value::AllFilter(), dht::Where{}.userType(std::string(DPASTE_USER_TYPE))
    );
}

std::vector<dht::Blob> Node::get(const std::string& code) {
    auto values = node_.get(dht::InfoHash::get(code),
            dht::Value::AllFilter(),
            dht::Where{}.userType(DPASTE_USER_TYPE)).get();
    std::vector<dht::Blob> blobs (values.size());
    std::transform(values.begin(), values.end(), blobs.begin(), [] (const decltype(values)::value_type& value) {
        return value->data;
    });
    return blobs;
}

} /* dpaste */
