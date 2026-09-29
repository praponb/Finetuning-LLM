SELECT
    d.channel,
    d.status,
    COUNT(*) AS rows_total,
    ROUND(SUM(a.unit_price), 2) AS total_unit_price,
    GROUPING_ID() AS level_id
FROM order_items a
INNER JOIN orders d ON a.order_id = d.id
GROUP BY ROLLUP (d.channel, d.status)
ORDER BY channel NULLS FIRST
