SELECT id, status, amount, SUM(amount) OVER (PARTITION BY status) AS status_sum_amount FROM orders ORDER BY status DESC
