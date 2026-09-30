select o.id, o.customer_id, c.email, s.region, s.city from orders o inner join customers c on c.id = o.customer_id left outer join stores s on o.store_id = s.id;
