SELECT product_id, SUM(quantity) total_quantity, MAX(unit_price) - MIN(unit_price) unit_price_range
FROM order_items
WHERE unit_price > 240
GROUP BY product_id
HAVING total_quantity < 38
ORDER BY product_id DESC
LIMIT 5;
