SELECT
    product_id,
    order_id,
    MAX(unit_price) - MIN(unit_price) AS unit_price_range,
    ROUND(AVG(quantity), 2) AS avg_quantity,
    COUNT(*) AS order_item_count
FROM order_items
WHERE quantity < 5 AND (quantity = 7 OR order_id IN (9, 33))
GROUP BY product_id, order_id
HAVING ROUND(AVG(quantity), 2) > 73
ORDER BY order_id, 1 DESC
LIMIT 50;
