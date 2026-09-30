SELECT
    o.channel,
    src.carrier,
    COUNT(*) AS cnt,
    MAX(src.cost) AS max_cost
FROM shipments src
LEFT JOIN orders o ON src.order_id = o.id
GROUP BY GROUPING SETS ((o.channel, src.carrier), (o.channel))
ORDER BY channel NULLS FIRST;
