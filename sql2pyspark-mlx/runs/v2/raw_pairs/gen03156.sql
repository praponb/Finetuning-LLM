select round(stddev(cost), 2) as stddev_cost, sort_array(collect_set(status)) as status_list from shipments
