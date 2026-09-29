SELECT
    id,
    product_id,
    unit_price,
    SUM(unit_price) OVER (PARTITION BY product_id) AS product_id_sum_unit_price
FROM order_items
WHERE product_id = 28
