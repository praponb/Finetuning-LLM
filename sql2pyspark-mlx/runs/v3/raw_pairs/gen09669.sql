select status, count(distinct cost) as unique_statuss, sort_array(collect_set(carrier)) as carrier_list from shipments group by 1
