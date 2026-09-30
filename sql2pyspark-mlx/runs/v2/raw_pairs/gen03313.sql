SELECT o.id, o.store_id, o.customer_id FROM orders o WHERE o.store_id NOT IN (SELECT id FROM stores WHERE opened_date BETWEEN '2023-07-12' AND '2024-09-13');
