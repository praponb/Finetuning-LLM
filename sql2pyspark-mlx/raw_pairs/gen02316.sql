SELECT
    order_id,
    COUNT(DISTINCT quantity) AS unique_orders,
    MAX(unit_price) - MIN(unit_price) AS unit_price_range,
    ROUND(AVG(unit_price), 2) AS avg_unit_price
FROM order_items
WHERE quantity = 6
GROUP BY order_id
HAVING MAX(quantity) > 3.8;
