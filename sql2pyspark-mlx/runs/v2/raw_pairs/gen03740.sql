SELECT
    order_id,
    product_id,
    MAX(unit_price) AS max_unit_price,
    ROUND(STDDEV(unit_price), 2) AS stddev_unit_price
FROM order_items
GROUP BY 1, 2;
