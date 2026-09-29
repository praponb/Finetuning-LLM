SELECT
    o.status,
    CASE WHEN o.discount >= 0.35 THEN 'high' WHEN o.discount >= 0.18 THEN 'mid' ELSE 'low' END AS discount_bucket,
    MIN(p.paid_at) AS first_paid_at,
    SUM(p.amount) AS total_amount
FROM payments p
INNER JOIN orders o ON o.id = p.order_id
GROUP BY o.status, CASE WHEN o.discount >= 0.35 THEN 'high' WHEN o.discount >= 0.18 THEN 'mid' ELSE 'low' END
ORDER BY total_amount DESC, discount_bucket DESC, status
LIMIT 20;
