SELECT order_id, product_id, MAX(unit_price) - MIN(unit_price) AS unit_price_range
FROM order_items
WHERE unit_price > 280 AND quantity = 8
GROUP BY CUBE(order_id, product_id)
ORDER BY order_id DESC
