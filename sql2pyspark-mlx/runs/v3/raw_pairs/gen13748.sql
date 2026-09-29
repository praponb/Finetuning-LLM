SELECT
    product_id,
    MAX(unit_price) - MIN(unit_price) AS unit_price_range,
    ROUND(STDDEV(unit_price), 2) AS stddev_unit_price,
    COUNT(*) AS num_order_items
FROM order_items
WHERE NOT (order_id IN (17, 31, 34) AND unit_price IS NOT NULL)
GROUP BY product_id
HAVING MAX(unit_price) - MIN(unit_price) < 66
