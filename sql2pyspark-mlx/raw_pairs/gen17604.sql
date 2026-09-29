SELECT product_id, AVG(unit_price) AS avg_unit_price
FROM order_items
WHERE unit_price BETWEEN 200 AND 230 AND product_id IS NULL AND quantity != 4
GROUP BY 1
