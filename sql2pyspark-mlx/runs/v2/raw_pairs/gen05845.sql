SELECT order_id, SUM(unit_price) AS total_unit_price, DENSE_RANK() OVER (ORDER BY SUM(unit_price) DESC) AS rnk
FROM order_items
WHERE product_id = 29
GROUP BY order_id
ORDER BY order_id DESC, total_unit_price
LIMIT 20
