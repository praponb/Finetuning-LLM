select o.id, o.customer_id, s.city, c.age from orders o left join stores s on o.store_id = s.id left join customers c on c.id = o.customer_id where month(s.opened_date) in (2, 10, 11);
