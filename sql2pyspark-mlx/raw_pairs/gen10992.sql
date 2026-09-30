select o.id, o.customer_id, s.store_name from orders o left join stores s on o.store_id = s.id where s.region in ('North', 'Central', 'East') order by store_name asc, o.customer_id asc;
