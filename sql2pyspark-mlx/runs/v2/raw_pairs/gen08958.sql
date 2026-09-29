SELECT
    o.channel,
    DATE_TRUNC('MONTH', o.order_date) AS order_month,
    MAX(s.shipped_date) AS latest_shipped_date,
    MAX(s.cost) AS max_cost
FROM shipments s
LEFT OUTER JOIN orders o ON s.order_id = o.id
WHERE o.store_id IS NULL
GROUP BY o.channel, DATE_TRUNC('MONTH', o.order_date)
ORDER BY channel DESC, order_month
