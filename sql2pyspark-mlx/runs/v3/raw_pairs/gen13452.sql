SELECT o.channel, COUNT(oi.id) AS order_item_count, MIN(oi.unit_price) AS min_unit_price
FROM orders o
LEFT OUTER JOIN order_items oi ON o.id = oi.order_id
GROUP BY o.channel
HAVING order_item_count >= 5
ORDER BY min_unit_price, channel
LIMIT 5;
