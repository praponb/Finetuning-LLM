SELECT product_id, AVG(unit_price) AS avg_unit_price, SUM(quantity * unit_price) AS revenue
FROM order_items
GROUP BY 1
