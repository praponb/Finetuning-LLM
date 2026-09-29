SELECT order_id, SUM(CASE WHEN status = 'SUCCESS' THEN amount ELSE 0 END) AS success_amount, MAX(amount) - MIN(amount) AS amount_range
FROM payments
GROUP BY order_id
HAVING COUNT(*) > 2
ORDER BY amount_range, order_id
LIMIT 3
