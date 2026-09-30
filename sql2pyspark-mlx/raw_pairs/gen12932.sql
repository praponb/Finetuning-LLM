SELECT
    *,
    MIN(quantity) OVER w_part AS running_min_quantity,
    AVG(quantity) OVER w_part AS running_avg_quantity,
    MAX(quantity) OVER w_part AS running_max_quantity,
    ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity, id) AS row_seq
FROM inventory
WINDOW w_part AS (PARTITION BY product_id ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
