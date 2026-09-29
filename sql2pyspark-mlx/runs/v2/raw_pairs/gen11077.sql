SELECT COUNT(*) AS total_count, SUM(unit_price * quantity) AS revenue, COUNT(unit_price) AS unit_price_count
FROM order_items
WHERE order_id = 28 AND product_id IS NULL
