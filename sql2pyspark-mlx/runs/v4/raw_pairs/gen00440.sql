SELECT p.id, p.order_id, o.customer_id, o.channel, s.store_name FROM payments p INNER JOIN orders o ON p.order_id = o.id FULL OUTER JOIN stores s ON o.store_id = s.id WHERE p.paid_at < '2023-08-03'
