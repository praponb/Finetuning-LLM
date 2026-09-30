SELECT id, customer_id FROM orders WHERE order_date BETWEEN '2024-08-01' AND '2025-08-01' UNION ALL SELECT id, customer_id FROM orders WHERE discount > 0.25;
