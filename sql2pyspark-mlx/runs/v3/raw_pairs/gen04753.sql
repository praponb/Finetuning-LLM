SELECT
    o.customer_id,
    CASE WHEN o.shipping_fee >= 32 THEN 'high' WHEN o.shipping_fee >= 12 THEN 'mid' ELSE 'low' END AS shipping_fee_bucket,
    AVG(s.cost) AS avg_cost,
    ROUND((100 * SUM(CASE WHEN s.carrier = 'DHL' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_dhl
FROM shipments s
FULL OUTER JOIN orders o ON s.order_id = o.id
LEFT JOIN customers c ON c.id = o.customer_id
WHERE NOT c.is_active
GROUP BY o.customer_id, CASE WHEN o.shipping_fee >= 32 THEN 'high' WHEN o.shipping_fee >= 12 THEN 'mid' ELSE 'low' END
