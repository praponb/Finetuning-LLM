select status, count(*) as total_count, concat_ws(', ', sort_array(collect_set(status))) as status_values from shipments group by status having count(*) < 3;
