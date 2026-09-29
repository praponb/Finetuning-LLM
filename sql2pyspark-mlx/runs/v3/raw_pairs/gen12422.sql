SELECT product_id, avg_unit_price, unit_price_range
FROM (
    SELECT product_id, AVG(unit_price) AS avg_unit_price, MAX(unit_price) - MIN(unit_price) AS unit_price_range
    FROM order_items
    WHERE order_id = 16
    GROUP BY product_id
) AS sub
WHERE unit_price_range > 23
ORDER BY product_id, 2 DESC
