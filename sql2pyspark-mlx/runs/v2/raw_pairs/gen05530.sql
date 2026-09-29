select o.id, o.store_id, c.email from orders o join customers c on o.customer_id = c.id where o.channel in ('store', 'mobile', 'web') and o.store_id = 30 order by store_id asc, c.email asc
