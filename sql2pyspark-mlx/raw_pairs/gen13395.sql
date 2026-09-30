SELECT product_id, COUNT(unit_price) AS unit_price_count, COUNT(DISTINCT quantity) AS unique_orders
FROM order_items
WHERE unit_price IS NOT NULL AND quantity > 4
GROUP BY product_id
ORDER BY product_id DESC
LIMIT 10;
