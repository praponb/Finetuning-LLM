SELECT status, COUNT(amount) AS amount_count, MAX(amount) AS max_amount
FROM payments
WHERE status = 'FAILED' AND order_id IS NOT NULL
GROUP BY status
HAVING COUNT(CASE WHEN method = 'paypal' THEN 1 END) > 5;
