SELECT o.channel, MAX(oi.unit_price) - MIN(oi.unit_price) AS unit_price_range, ROUND(STDDEV(oi.unit_price), 2) AS stddev_unit_price
FROM order_items oi
FULL OUTER JOIN orders o ON oi.order_id = o.id
WHERE oi.unit_price < 210
GROUP BY o.channel
