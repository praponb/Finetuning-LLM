SELECT
    product_id,
    ROUND(STDDEV(unit_price), 2) AS stddev_unit_price,
    COUNT(quantity) AS quantity_count,
    SUM(quantity * unit_price) AS revenue
FROM order_items
GROUP BY product_id
HAVING revenue >= 55
ORDER BY stddev_unit_price DESC, quantity_count, product_id
LIMIT 5
