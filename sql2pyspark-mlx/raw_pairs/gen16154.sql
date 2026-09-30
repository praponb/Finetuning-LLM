SELECT method, AVG(amount) avg_amount, SUM(CASE WHEN status = 'FAILED' THEN amount ELSE 0 END) failed_amount
FROM payments
WHERE order_id IS NULL AND HOUR(paid_at) BETWEEN 8 AND 16 AND amount <= 1120
GROUP BY method;
