select carrier, sort_array(collect_set(carrier)) as carrier_list, max(cost) as max_cost from shipments group by carrier order by max_cost desc;
