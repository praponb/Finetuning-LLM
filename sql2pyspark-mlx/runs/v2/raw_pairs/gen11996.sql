select o.id, o.status, o.store_id, c.segment, c.city, c.credit_limit, case when o.channel = 'web' then 1 else 0 end as is_web from orders o join customers c on o.customer_id = c.id;
