SELECT c.id, c.name, c.age FROM customers c WHERE c.is_active AND NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id AND o.order_date > '2023-10-01') ORDER BY id DESC, age DESC LIMIT 20;
