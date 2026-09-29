WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        DATEDIFF(CURRENT_DATE(), last_restock) AS days_since_last_restock
    FROM inventory
    WHERE quantity >= 170
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        store_id,
        product_id,
        FIRST_VALUE(days_since_last_restock, TRUE) OVER (PARTITION BY product_id, store_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_days_since_last_restock,
        LEAD(days_since_last_restock, 1, 0) OVER (PARTITION BY store_id ORDER BY quantity, id) AS next_days_since_last_restock
    FROM filtered
),
enriched_inventory AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        next_days_since_last_restock,
        NTILE(10) OVER (ORDER BY first_known_days_since_last_restock DESC, id) AS first_known_days_since_last_restock_bucket,
        SUM(first_known_days_since_last_restock) OVER (PARTITION BY store_id ORDER BY next_days_since_last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_first_known_days_since_last_restock,
        DENSE_RANK() OVER (ORDER BY next_days_since_last_restock) AS next_days_since_last_restock_dense_rank,
        SUM(next_days_since_last_restock) OVER (PARTITION BY product_id ORDER BY first_known_days_since_last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_next_days_since_last_restock,
        STDDEV_SAMP(first_known_days_since_last_restock) OVER (PARTITION BY product_id, store_id) AS product_id_stddev_samp_first_known_days_since_last_restock
    FROM enriched
),
enriched_inventory_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_id,
        next_days_since_last_restock_dense_rank,
        cumulative_next_days_since_last_restock,
        product_id_stddev_samp_first_known_days_since_last_restock,
        AVG(next_days_since_last_restock) OVER (PARTITION BY store_id ORDER BY next_days_since_last_restock, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_avg_next_days_since_last_restock,
        SUM(next_days_since_last_restock) OVER (PARTITION BY store_id ORDER BY next_days_since_last_restock, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_sum_next_days_since_last_restock,
        MIN(next_days_since_last_restock) OVER (PARTITION BY store_id ORDER BY next_days_since_last_restock, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_min_next_days_since_last_restock
    FROM enriched_inventory
),
enriched_inventory_3 AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(product_id_stddev_samp_first_known_days_since_last_restock) OVER (PARTITION BY store_id ORDER BY product_id_stddev_samp_first_known_days_since_last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_product_id_stddev_samp_first_known_days_since_last_restock,
        MAX(product_id_stddev_samp_first_known_days_since_last_restock) OVER (PARTITION BY store_id ORDER BY product_id_stddev_samp_first_known_days_since_last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_product_id_stddev_samp_first_known_days_since_last_restock,
        MIN(product_id_stddev_samp_first_known_days_since_last_restock) OVER (PARTITION BY store_id ORDER BY product_id_stddev_samp_first_known_days_since_last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_product_id_stddev_samp_first_known_days_since_last_restock,
        COUNT(product_id_stddev_samp_first_known_days_since_last_restock) OVER (PARTITION BY store_id ORDER BY product_id_stddev_samp_first_known_days_since_last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_product_id_stddev_samp_first_known_days_since_last_restock,
        NTILE(4) OVER (PARTITION BY store_id ORDER BY last4_sum_next_days_since_last_restock, id) AS last4_sum_next_days_since_last_restock_bucket
    FROM enriched_inventory_2
    WHERE last4_min_next_days_since_last_restock >= 36
),
summary AS (
    -- Multi-level aggregation
    SELECT
        store_id,
        MAX(running_max_product_id_stddev_samp_first_known_days_since_last_restock) AS max_running_max_product_id_stddev_samp_first_known_days_since_last_restock,
        ROUND(AVG(last4_min_next_days_since_last_restock), 2) AS avg_last4_min_next_days_since_last_restock,
        PERCENTILE_APPROX(running_avg_product_id_stddev_samp_first_known_days_since_last_restock, ARRAY(0.25, 0.5, 0.75)) AS running_avg_product_id_stddev_samp_first_known_days_since_last_restock_quartiles,
        ROUND(AVG(last4_avg_next_days_since_last_restock), 2) AS avg_last4_avg_next_days_since_last_restock,
        PERCENTILE_APPROX(last4_sum_next_days_since_last_restock, 0.95) AS last4_sum_next_days_since_last_restock_p95,
        PERCENTILE_APPROX(running_min_product_id_stddev_samp_first_known_days_since_last_restock, 0.95) AS running_min_product_id_stddev_samp_first_known_days_since_last_restock_p95
    FROM enriched_inventory_3
    GROUP BY store_id
    HAVING ROUND(AVG(last4_min_next_days_since_last_restock), 2) >= 0
)
SELECT
    store_id,
    avg_last4_min_next_days_since_last_restock,
    avg_last4_avg_next_days_since_last_restock,
    last4_sum_next_days_since_last_restock_p95,
    running_min_product_id_stddev_samp_first_known_days_since_last_restock_p95
FROM summary
ORDER BY last4_sum_next_days_since_last_restock_p95 DESC
