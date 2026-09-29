WITH scored AS (
    SELECT
        id,
        product_id,
        AVG(quantity) OVER (PARTITION BY MONTH(last_restock), store_id ORDER BY quantity, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS quantity_avg_2,
        SUM(quantity) OVER w_part AS running_quantity,
        NTILE(4) OVER w_part_2 AS quantity_bucket
    FROM inventory
    WINDOW w_part AS (PARTITION BY store_id, product_id ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_part_2 AS (PARTITION BY store_id ORDER BY quantity, id)
)
SELECT
    product_id,
    PERCENTILE_APPROX(quantity_avg_2, 0.9) AS quantity_avg_2_p90,
    COUNT(DISTINCT product_id) AS distinct_product_ids,
    APPROX_PERCENTILE(running_quantity, 0.5) AS running_quantity_p50,
    ROUND(AVG(running_quantity), 2) AS avg_running_quantity,
    MAX(quantity_bucket) AS max_quantity_bucket
FROM scored
GROUP BY 1
