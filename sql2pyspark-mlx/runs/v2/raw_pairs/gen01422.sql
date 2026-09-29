SELECT id, status, order_date, amount, SUM(amount) OVER (PARTITION BY status ORDER BY order_date, id) AS running_amount FROM orders WHERE customer_id IS NULL;
