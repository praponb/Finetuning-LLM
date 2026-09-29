SELECT
    order_id,
    product_id,
    COUNT(DISTINCT quantity) AS unique_products,
    SUM(quantity) AS total_quantity,
    MAX(unit_price) - MIN(unit_price) AS unit_price_range
FROM order_items
WHERE quantity > 5
GROUP BY order_id, product_id
HAVING SUM(unit_price * quantity) < 55
ORDER BY unit_price_range DESC, order_id DESC;
