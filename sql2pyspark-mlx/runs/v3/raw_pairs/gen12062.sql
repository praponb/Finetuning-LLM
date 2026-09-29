SELECT
    id,
    product_id,
    unit_price,
    SUM(unit_price) OVER (PARTITION BY product_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_unit_price
FROM order_items
WHERE unit_price > 210
ORDER BY id;
