select o.amount, o.discount, o.customer_id, s.store_name from orders o join stores s on o.store_id = s.id where s.region in ('North', 'South', 'Central');
