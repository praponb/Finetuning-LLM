SELECT CASE WHEN orders.discount >= 0.32 THEN 'high' WHEN orders.discount >= 0.07 THEN 'mid' ELSE 'low' END AS discount_bucket, ROUND((100 * SUM(CASE WHEN payments.status = 'REFUNDED' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_refunded, COUNT(CASE WHEN payments.method = 'card' THEN 1 END) AS card_count
FROM payments
LEFT JOIN orders ON payments.order_id = orders.id
WHERE payments.amount >= 1340
GROUP BY CASE WHEN orders.discount >= 0.32 THEN 'high' WHEN orders.discount >= 0.07 THEN 'mid' ELSE 'low' END
ORDER BY 1
