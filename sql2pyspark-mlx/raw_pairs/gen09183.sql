select s.city, min(o.order_date) first_order_date from orders o left join stores s on o.store_id = s.id group by s.city order by first_order_date desc;
