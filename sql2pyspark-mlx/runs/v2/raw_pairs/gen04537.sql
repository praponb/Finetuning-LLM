SELECT order_id, MAX(unit_price) max_unit_price
FROM order_items
WHERE quantity >= 5
GROUP BY order_id
ORDER BY order_id DESC
