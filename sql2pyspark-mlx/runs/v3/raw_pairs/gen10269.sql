select account_type, concat_ws(', ', sort_array(collect_set(status))) as status_values from accounts where status in ('FROZEN', 'OPEN') group by account_type;
