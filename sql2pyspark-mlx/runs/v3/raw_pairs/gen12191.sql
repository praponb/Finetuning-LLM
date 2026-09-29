SELECT order_id, COUNT(quantity) AS quantity_count, COUNT(unit_price) AS unit_price_count
FROM order_items
GROUP BY order_id
ORDER BY quantity_count ASC, order_id ASC
LIMIT 10
