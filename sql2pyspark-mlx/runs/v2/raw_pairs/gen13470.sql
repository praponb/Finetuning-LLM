SELECT product_id, ROUND(AVG(quantity), 2) AS avg_quantity
FROM order_items
WHERE unit_price BETWEEN 210 AND 230
GROUP BY product_id
