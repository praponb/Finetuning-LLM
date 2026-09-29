WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN quantity >= 192 THEN 'High' WHEN quantity >= 66 THEN 'Medium' ELSE 'Low' END AS quantity_level,
        FLOOR(quantity) AS quantity_whole
    FROM inventory
    WHERE last_restock > TO_DATE('2023-02-05')
      AND quantity BETWEEN 185 AND 188
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(quantity_whole, 1) OVER (PARTITION BY product_id ORDER BY quantity, id) AS prev_quantity_whole,
        SUM(quantity) OVER (ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_quantity,
        LEAD(quantity, 1) OVER (PARTITION BY product_id ORDER BY quantity_whole, id) AS next_quantity,
        NTILE(3) OVER w_ordered AS quantity_bucket
    FROM base
    WINDOW w_ordered AS (PARTITION BY store_id ORDER BY quantity, id)
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        next_quantity / NULLIF(quantity_whole, 0) AS next_quantity_per_quantity_whole,
        quantity_whole * cumulative_quantity AS quantity_whole_times_cumulative_quantity
    FROM metrics
    WHERE last_restock >= DATE '2023-03-02'
),
kept_keys AS (
    -- Set difference on keys
    SELECT store_id, id
    FROM filtered
    EXCEPT ALL
    SELECT store_id, id
    FROM inventory
    WHERE store_id IN (1, 2, 5, 6)
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN kept_keys k ON s.store_id = k.store_id AND s.id = k.id
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        quantity,
        prev_quantity_whole,
        cumulative_quantity,
        next_quantity,
        quantity_bucket,
        next_quantity_per_quantity_whole,
        RANK() OVER (ORDER BY quantity NULLS FIRST) AS quantity_rank,
        RANK() OVER (PARTITION BY product_id ORDER BY next_quantity_per_quantity_whole NULLS FIRST) AS next_quantity_per_quantity_whole_rank,
        AVG(quantity_whole) OVER w_ordered AS store_id_avg_quantity_whole,
        PERCENT_RANK() OVER w_grp AS prev_quantity_whole_percent_rank,
        MIN(quantity_whole) OVER w_part AS last_restock_period_min_quantity_whole
    FROM kept_rows
    WHERE store_id IN (2, 6)
    WINDOW w_ordered AS (PARTITION BY store_id), w_grp AS (PARTITION BY product_id ORDER BY prev_quantity_whole DESC), w_part AS (PARTITION BY DATE_TRUNC('WEEK', last_restock))
),
filtered_inventory AS (
    -- Keep recent, valid rows
    SELECT *, quantity_rank / NULLIF(prev_quantity_whole, 0) AS quantity_rank_per_prev_quantity_whole
    FROM scored
    WHERE cumulative_quantity = 1018
),
metrics_inventory AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        next_quantity,
        quantity_bucket,
        next_quantity_per_quantity_whole,
        prev_quantity_whole_percent_rank,
        last_restock_period_min_quantity_whole,
        quantity_rank_per_prev_quantity_whole,
        CUME_DIST() OVER (ORDER BY quantity_rank_per_prev_quantity_whole) AS quantity_rank_per_prev_quantity_whole_cume_dist,
        RANK() OVER (ORDER BY quantity NULLS FIRST) AS quantity_rank_2,
        RANK() OVER w_hist AS next_quantity_rank
    FROM filtered_inventory
    WINDOW w_hist AS (ORDER BY next_quantity DESC NULLS LAST)
),
daily AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN next_quantity >= 221 THEN 'HIGH' ELSE 'LOW' END AS next_quantity_tier,
        APPROX_PERCENTILE(quantity_rank_per_prev_quantity_whole_cume_dist, ARRAY(0.25, 0.5, 0.75)) AS quantity_rank_per_prev_quantity_whole_cume_dist_quartiles,
        COUNT(*) AS row_count,
        APPROX_PERCENTILE(next_quantity, ARRAY(0.25, 0.5, 0.75)) AS next_quantity_quartiles,
        PERCENTILE_APPROX(prev_quantity_whole_percent_rank, 0.5) AS prev_quantity_whole_percent_rank_p50,
        PERCENTILE_APPROX(quantity_rank_per_prev_quantity_whole_cume_dist, ARRAY(0.25, 0.5, 0.75)) AS quantity_rank_per_prev_quantity_whole_cume_dist_quartiles_2,
        MAX(quantity_rank_per_prev_quantity_whole_cume_dist) AS max_quantity_rank_per_prev_quantity_whole_cume_dist
    FROM metrics_inventory
    GROUP BY ROLLUP (CASE WHEN next_quantity >= 221 THEN 'HIGH' ELSE 'LOW' END)
)
SELECT
    next_quantity_tier,
    quantity_rank_per_prev_quantity_whole_cume_dist_quartiles,
    row_count,
    next_quantity_quartiles,
    prev_quantity_whole_percent_rank_p50,
    quantity_rank_per_prev_quantity_whole_cume_dist_quartiles_2,
    max_quantity_rank_per_prev_quantity_whole_cume_dist
FROM daily
