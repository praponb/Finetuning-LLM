SELECT o.id, o.status FROM orders o WHERE o.store_id NOT IN (SELECT id FROM stores WHERE opened_date < '2023-10-01') ORDER BY id ASC
