SELECT status, MIN(amount) min_amount
FROM payments
WHERE order_id IN (14, 18, 22, 35) AND TO_DATE(paid_at) < '2023-05-01'
GROUP BY status
HAVING MAX(amount) > 1440
ORDER BY min_amount DESC;
