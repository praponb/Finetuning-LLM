SELECT product_id, SUM(unit_price * quantity) AS revenue
FROM order_items
GROUP BY product_id
ORDER BY product_id, revenue DESC
