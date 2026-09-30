SELECT product_id, MAX(unit_price) - MIN(unit_price) AS unit_price_range, COUNT(DISTINCT quantity) AS unique_products
FROM order_items
GROUP BY product_id
ORDER BY unique_products ASC, product_id ASC
LIMIT 10;
