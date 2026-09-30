SELECT o.id, o.store_id, o.amount, o.discount FROM orders o WHERE o.order_date <= '2024-08-18' AND o.store_id IN (SELECT id FROM stores WHERE opened_date BETWEEN '2024-03-10' AND '2024-11-27')
