select s.city, count(o.id) as order_count, sort_array(collect_set(o.channel)) as channel_list from stores s inner join orders o on o.store_id = s.id group by s.city;
