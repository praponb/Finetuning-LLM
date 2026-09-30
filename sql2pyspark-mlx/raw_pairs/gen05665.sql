SELECT
    product_id,
    COUNT(quantity) AS quantity_count,
    ROUND(STDDEV(unit_price), 2) AS stddev_unit_price,
    SUM(unit_price * quantity) AS revenue
FROM order_items
GROUP BY product_id
HAVING ROUND(STDDEV(unit_price), 2) >= 59
