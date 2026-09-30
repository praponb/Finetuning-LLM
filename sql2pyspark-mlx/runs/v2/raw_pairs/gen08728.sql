SELECT id, customer_id FROM orders WHERE order_date BETWEEN '2023-09-01' AND '2024-10-01' UNION ALL SELECT id, customer_id FROM orders WHERE discount <= 0.34;
