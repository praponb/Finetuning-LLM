SELECT c.id, c.age FROM customers c WHERE c.name IS NOT NULL AND EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id AND YEAR(o.order_date) >= 2025)
