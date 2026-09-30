SELECT c.id, c.is_active FROM customers c WHERE c.is_active = TRUE AND NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id AND o.order_date >= '2023-06-01') ORDER BY c.id;
