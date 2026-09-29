SELECT product_id, SUM(unit_price) AS total_unit_price
FROM order_items
WHERE (quantity < 6 AND order_id = 5) OR unit_price >= 270
GROUP BY product_id;
