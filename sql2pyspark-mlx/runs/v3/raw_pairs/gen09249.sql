select status, concat_ws(', ', sort_array(collect_set(channel))) channel_values from orders where discount <= 0.17 group by status order by 2 desc, status limit 10
