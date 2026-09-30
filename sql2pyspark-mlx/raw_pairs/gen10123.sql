SELECT
    order_id,
    ROUND(STDDEV(quantity), 2) AS stddev_quantity,
    MIN(unit_price) AS min_unit_price,
    COUNT(unit_price) AS unit_price_count
FROM order_items
WHERE unit_price BETWEEN 100 AND 310
GROUP BY order_id
HAVING min_unit_price > 170;
