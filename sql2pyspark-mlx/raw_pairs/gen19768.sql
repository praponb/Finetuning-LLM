WITH enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        last_restock,
        NTILE(10) OVER (PARTITION BY store_id ORDER BY quantity, id) AS quantity_bucket,
        LEAD(quantity, 2) OVER (ORDER BY last_restock, id) AS next_quantity,
        STDDEV_SAMP(quantity) OVER (PARTITION BY product_id, store_id) AS product_id_stddev_samp_quantity,
        LEAD(quantity, 1) OVER (PARTITION BY store_id, product_id ORDER BY last_restock, id) AS next_quantity_2
    FROM inventory
    WHERE quantity > 231
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY next_quantity_2 DESC) AS next_quantity_2_cume_dist,
        AVG(quantity_bucket) OVER (ORDER BY product_id_stddev_samp_quantity, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS quantity_bucket_avg_3,
        CUME_DIST() OVER (ORDER BY product_id_stddev_samp_quantity) AS product_id_stddev_samp_quantity_cume_dist
    FROM enriched
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        STDDEV_SAMP(quantity_bucket) OVER w_hist AS last_restock_period_stddev_samp_quantity_bucket,
        AVG(quantity_bucket) OVER (ORDER BY quantity_bucket, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS quantity_bucket_avg_6_prior,
        FIRST_VALUE(next_quantity_2_cume_dist) IGNORE NULLS OVER (ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_next_quantity_2_cume_dist
    FROM with_windows
    WINDOW w_hist AS (PARTITION BY DATE_TRUNC('month', last_restock))
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM metrics
    INTERSECT
    SELECT id
    FROM inventory
    WHERE quantity BETWEEN 123 AND 194
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    INNER JOIN matched_keys k ON s.id = k.id
)
SELECT
    quantity_bucket,
    next_quantity_2,
    last_restock_period_stddev_samp_quantity_bucket,
    quantity_bucket_avg_6_prior,
    first_known_next_quantity_2_cume_dist
FROM reconciled
ORDER BY first_known_next_quantity_2_cume_dist DESC NULLS LAST, next_quantity_2
