SELECT order_id, ROUND(STDDEV(unit_price), 2) AS stddev_unit_price, SUM(quantity * unit_price) AS revenue
FROM order_items
WHERE product_id IS NULL
GROUP BY order_id
ORDER BY revenue DESC, order_id
LIMIT 5
