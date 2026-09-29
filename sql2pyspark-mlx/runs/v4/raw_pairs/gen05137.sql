SELECT
    o.channel,
    o.status,
    SUM(CASE WHEN s.carrier = 'DHL' THEN 1 ELSE 0 END) dhl_count,
    COUNT(s.cost) cost_count
FROM shipments s
INNER JOIN orders o ON s.order_id = o.id
WHERE o.status <> 'SHIPPED'
GROUP BY o.channel, o.status;
