SELECT
    *,
    DENSE_RANK() OVER (ORDER BY quantity) AS quantity_dense_rank,
    ROUND(quantity / SUM(quantity) OVER (PARTITION BY product_id, order_id), 4) AS quantity_share,
    SUM(unit_price) OVER (PARTITION BY order_id) AS order_id_sum_unit_price,
    MIN(unit_price) OVER (PARTITION BY product_id) AS product_id_min_unit_price,
    NTILE(3) OVER (ORDER BY quantity, id) AS quantity_bucket
FROM order_items;
