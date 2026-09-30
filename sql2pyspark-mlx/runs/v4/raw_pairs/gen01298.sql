WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity DESC, id) AS latest_rank
    FROM inventory
    QUALIFY latest_rank = 1
)
SELECT
    DATE_TRUNC('YEAR', last_restock) AS year_start,
    store_id,
    APPROX_PERCENTILE(quantity, ARRAY(0.25, 0.5, 0.75)) AS quantity_quartiles,
    BIT_AND(quantity) AS quantity_bit_and,
    COUNT(DISTINCT product_id) AS distinct_product_ids,
    COUNT(DISTINCT id) AS distinct_ids,
    APPROX_PERCENTILE(quantity, ARRAY(0.25, 0.5, 0.75)) AS quantity_quartiles_2,
    APPROX_PERCENTILE(quantity, ARRAY(0.25, 0.5, 0.75)) AS quantity_quartiles_3
FROM ranked
GROUP BY GROUPING SETS ((DATE_TRUNC('YEAR', last_restock), store_id), (DATE_TRUNC('YEAR', last_restock)))
ORDER BY store_id
