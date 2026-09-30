SELECT
    id,
    store_id,
    MIN(quantity) OVER w_running AS running_min_quantity,
    SUM(quantity) OVER w_running AS running_sum_quantity,
    MAX(quantity) OVER w_running AS running_max_quantity
FROM inventory
WINDOW w_running AS (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
