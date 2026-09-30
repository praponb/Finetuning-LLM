SELECT status, ROUND(AVG(amount), 2) avg_amount
FROM payments
WHERE method = 'cash' AND status = 'FAILED' AND order_id = 18
GROUP BY 1
ORDER BY avg_amount DESC;
