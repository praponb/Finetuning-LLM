select c.is_active, sort_array(collect_set(s.device)) as device_list from sessions s inner join customers c on s.customer_id = c.id where c.city != 'London' group by c.is_active
