SELECT order_id, SUM(CASE WHEN status = 'REFUNDED' THEN 1 ELSE 0 END) AS refunded_count
FROM payments
WHERE amount BETWEEN 620 AND 760 OR paid_at < '2023-07-01'
GROUP BY order_id;
