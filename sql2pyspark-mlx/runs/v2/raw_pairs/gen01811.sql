select o.id, o.customer_id, c.age, s.region from orders o left outer join customers c on o.customer_id = c.id inner join stores s on o.store_id = s.id;
