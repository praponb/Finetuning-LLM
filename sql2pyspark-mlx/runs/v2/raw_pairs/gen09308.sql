SELECT o.id, o.customer_id
FROM orders o
WHERE o.store_id NOT IN (SELECT id FROM stores WHERE opened_date <= '2024-02-24')
