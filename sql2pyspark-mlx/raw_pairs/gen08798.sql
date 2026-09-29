SELECT
    o.status,
    CASE WHEN o.amount >= 1580 THEN 'high' WHEN o.amount >= 540 THEN 'mid' ELSE 'low' END AS amount_bucket,
    COUNT(*) AS row_count,
    ROUND(AVG(oi.unit_price), 2) AS avg_unit_price
FROM order_items oi
INNER JOIN orders o ON oi.order_id = o.id
WHERE o.store_id IS NOT NULL
GROUP BY o.status, CASE WHEN o.amount >= 1580 THEN 'high' WHEN o.amount >= 540 THEN 'mid' ELSE 'low' END
