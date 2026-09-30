SELECT
    o.channel,
    o.status,
    COUNT(*) AS cnt,
    COUNT(DISTINCT s.cost) AS unique_carriers
FROM shipments s
LEFT OUTER JOIN orders o ON o.id = s.order_id
GROUP BY o.channel, o.status
ORDER BY cnt
