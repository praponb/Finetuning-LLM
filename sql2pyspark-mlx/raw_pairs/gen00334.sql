SELECT
    id,
    store_id,
    product_id,
    last_restock,
    MAX(quantity) OVER w_part running_max_quantity,
    MIN(quantity) OVER w_part running_min_quantity,
    SUM(quantity) OVER w_part running_sum_quantity,
    AVG(quantity) OVER w_part_2 quantity_avg_3
FROM inventory
WINDOW w_part AS (PARTITION BY store_id ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_part_2 AS (PARTITION BY product_id ORDER BY last_restock, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
