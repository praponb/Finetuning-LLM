SELECT
    o.channel,
    QUARTER(o.order_date) AS order_quarter,
    SUM(s.cost) AS total_cost,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(s.status))) AS status_values
FROM shipments s
JOIN orders o ON s.order_id = o.id
GROUP BY o.channel, QUARTER(o.order_date)
HAVING SUM(s.cost) >= 300
ORDER BY order_quarter, channel
LIMIT 5
