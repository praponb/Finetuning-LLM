WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(quantity * 0.9, 2) AS quantity_scaled
    FROM inventory
    WHERE last_restock >= DATE '2023-02-09'
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        quantity,
        RANK() OVER (PARTITION BY store_id, product_id ORDER BY quantity_scaled DESC NULLS FIRST) AS quantity_scaled_rank,
        AVG(quantity_scaled) OVER (PARTITION BY YEAR(last_restock), store_id ORDER BY last_restock, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS quantity_scaled_avg_3_prior,
        NTILE(3) OVER (ORDER BY quantity_scaled DESC, id) AS quantity_scaled_bucket,
        SUM(quantity_scaled) OVER (PARTITION BY store_id) AS store_id_sum_quantity_scaled
    FROM valid
    WHERE last_restock >= '2024-07-01'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(quantity_scaled_rank, TRUE) OVER (ORDER BY quantity_scaled_avg_3_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_quantity_scaled_rank,
        NTILE(4) OVER (ORDER BY quantity_scaled_bucket, id) AS quantity_scaled_bucket_bucket,
        RANK() OVER (ORDER BY quantity_scaled_rank DESC NULLS FIRST) AS quantity_scaled_rank_rank,
        SUM(quantity_scaled_bucket) OVER (ORDER BY quantity_scaled_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_quantity_scaled_bucket,
        SUM(quantity) OVER (ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_quantity
    FROM scored
),
valid_inventory AS (
    -- Keep recent, valid rows
    SELECT
        id,
        quantity,
        quantity_scaled_rank,
        quantity_scaled_avg_3_prior,
        quantity_scaled_bucket,
        store_id_sum_quantity_scaled,
        first_known_quantity_scaled_rank,
        quantity_scaled_rank_rank,
        running_quantity,
        quantity_scaled_rank_rank / NULLIF(quantity_scaled_avg_3_prior, 0) AS quantity_scaled_rank_rank_per_quantity_scaled_avg_3_prior
    FROM with_windows
    WHERE first_known_quantity_scaled_rank <> 5
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(running_quantity) OVER (ORDER BY quantity_scaled_bucket, id) AS prev_running_quantity,
        LAG(quantity_scaled_rank_rank) OVER (ORDER BY quantity_scaled_bucket, id) AS prev_quantity_scaled_rank_rank,
        LEAD(quantity_scaled_rank_rank) OVER (ORDER BY quantity_scaled_bucket, id) AS next_quantity_scaled_rank_rank,
        ROW_NUMBER() OVER (ORDER BY quantity_scaled_bucket, id) AS row_seq
    FROM valid_inventory
),
totals AS (
    -- Grouped metrics
    SELECT
        CASE WHEN row_seq >= 9 THEN 'HIGH' ELSE 'LOW' END AS row_seq_tier,
        CASE WHEN quantity >= 235 THEN 'HIGH' ELSE 'LOW' END AS quantity_tier,
        ROUND(AVG(first_known_quantity_scaled_rank), 2) AS avg_first_known_quantity_scaled_rank,
        COUNT(*) AS cnt,
        ROUND(AVG(quantity_scaled_rank), 2) AS avg_quantity_scaled_rank,
        COUNT(DISTINCT id) AS distinct_ids,
        PERCENTILE_APPROX(quantity_scaled_bucket, 0.5) AS quantity_scaled_bucket_p50,
        GROUPING_ID() AS gid
    FROM metrics
    GROUP BY GROUPING SETS ((CASE WHEN row_seq >= 9 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN quantity >= 235 THEN 'HIGH' ELSE 'LOW' END), (CASE WHEN row_seq >= 9 THEN 'HIGH' ELSE 'LOW' END), ())
)
SELECT
    row_seq_tier,
    quantity_tier,
    cnt,
    avg_quantity_scaled_rank,
    quantity_scaled_bucket_p50,
    gid
FROM totals
ORDER BY cnt
