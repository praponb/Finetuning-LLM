select month(delivered_date) as delivered_month, status, concat_ws(', ', sort_array(collect_set(status))) as status_values from shipments group by month(delivered_date), status;
