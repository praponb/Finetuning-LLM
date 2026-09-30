SELECT status, order_id, MIN(amount) AS min_amount
FROM payments
GROUP BY status, order_id
ORDER BY min_amount DESC;
