SELECT o.id, o.store_id FROM orders o WHERE o.customer_id IN (SELECT id FROM customers WHERE signup_date >= '2025-02-12') ORDER BY store_id DESC, o.id DESC LIMIT 10;
