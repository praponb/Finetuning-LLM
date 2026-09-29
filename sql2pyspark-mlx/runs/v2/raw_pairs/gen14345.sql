SELECT
    id,
    product_id,
    unit_price,
    SUM(unit_price) OVER (PARTITION BY product_id) AS product_id_sum_unit_price,
    COUNT(*) OVER (PARTITION BY product_id) AS product_id_count
FROM order_items;
