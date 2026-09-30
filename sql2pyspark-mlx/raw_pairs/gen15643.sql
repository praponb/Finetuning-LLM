select o.id, o.store_id, o.customer_id, s.sqft, c.age from orders o join stores s on s.id = o.store_id join customers c on o.customer_id = c.id where o.customer_id is not null and o.amount > 690
