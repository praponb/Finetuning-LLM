SELECT
    *,
    MAX(quantity) OVER (PARTITION BY store_id, product_id) AS store_id_max_quantity,
    SUM(quantity) OVER (PARTITION BY product_id) AS product_id_sum_quantity,
    SUM(quantity) OVER (PARTITION BY product_id ORDER BY quantity ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_quantity
FROM inventory;
