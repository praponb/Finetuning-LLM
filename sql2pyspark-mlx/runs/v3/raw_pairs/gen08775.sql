select o.id, o.order_date, s.store_name from orders o inner join stores s on o.store_id = s.id order by s.store_name;
