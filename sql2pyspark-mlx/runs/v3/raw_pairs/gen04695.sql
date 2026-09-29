SELECT order_id, COUNT(DISTINCT quantity) AS unique_products, SUM(unit_price * quantity) AS revenue
FROM order_items
GROUP BY order_id
ORDER BY 2 DESC
