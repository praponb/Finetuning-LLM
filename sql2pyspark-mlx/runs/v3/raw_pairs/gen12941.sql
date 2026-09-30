select c.is_active, concat_ws(', ', sort_array(collect_set(s.country))) country_values, count(*) num_sessions from sessions s left join customers c on s.customer_id = c.id group by c.is_active
