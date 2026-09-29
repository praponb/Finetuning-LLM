SELECT product_id, COUNT(DISTINCT unit_price) AS unique_products, MAX(unit_price) AS max_unit_price
FROM order_items
GROUP BY product_id
HAVING MIN(unit_price) >= 370
ORDER BY unique_products, product_id
