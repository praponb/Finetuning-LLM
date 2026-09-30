WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        product_id,
        sale_date,
        units,
        revenue,
        units + revenue revenue_per_unit
    FROM store_sales
    WHERE product_id IN (1, 6, 8)
),
enriched AS (
    -- Attach products attributes
    SELECT
        s.id,
        s.product_id,
        s.units,
        p.category,
        p.launch_date
    FROM filtered s
    JOIN products p
        ON s.product_id = p.id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        category,
        launch_date,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY units, id) row_seq,
        MAX(units) OVER (PARTITION BY product_id) product_id_max_units,
        AVG(units) OVER (ORDER BY launch_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) units_avg_3
    FROM enriched
),
with_windows_store_sales AS (
    -- Running and moving aggregates
    SELECT
        id,
        row_seq,
        units_avg_3,
        AVG(units_avg_3) OVER (PARTITION BY category ORDER BY launch_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) units_avg_3_avg_3,
        RANK() OVER (PARTITION BY category ORDER BY units_avg_3) units_avg_3_rank,
        ROUND(product_id_max_units / SUM(product_id_max_units) OVER (PARTITION BY YEAR(launch_date), category), 4) product_id_max_units_share,
        PERCENT_RANK() OVER (PARTITION BY YEAR(launch_date), category ORDER BY row_seq DESC NULLS LAST) row_seq_percent_rank
    FROM with_windows
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        units_avg_3,
        units_avg_3_avg_3,
        units_avg_3_rank,
        product_id_max_units_share,
        row_seq_percent_rank,
        CAST(row_seq_percent_rank AS INT) row_seq_percent_rank_int,
        row_seq * units_avg_3 row_seq_times_units_avg_3
    FROM with_windows_store_sales
    WHERE row_seq_percent_rank < 0.39
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        units_avg_3,
        product_id_max_units_share,
        row_seq_percent_rank,
        row_seq_percent_rank_int,
        row_seq_times_units_avg_3,
        NTILE(10) OVER (ORDER BY units_avg_3_avg_3, id) units_avg_3_avg_3_bucket,
        LAG(units_avg_3, 2) OVER (ORDER BY row_seq_percent_rank, id) prev_units_avg_3,
        SUM(units_avg_3_avg_3) OVER (ORDER BY units_avg_3_avg_3, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_units_avg_3_avg_3,
        LAST_VALUE(row_seq_times_units_avg_3) IGNORE NULLS OVER (ORDER BY units_avg_3, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_row_seq_times_units_avg_3,
        SUM(product_id_max_units_share) OVER (ORDER BY row_seq_times_units_avg_3, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_product_id_max_units_share
    FROM base
),
enriched_store_sales AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(row_seq_percent_rank) OVER w_recent running_row_seq_percent_rank,
        ROW_NUMBER() OVER (ORDER BY row_seq_times_units_avg_3 DESC, id) row_seq,
        FIRST_VALUE(last_known_row_seq_times_units_avg_3, TRUE) OVER (ORDER BY prev_units_avg_3, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_last_known_row_seq_times_units_avg_3
    FROM metrics
    WINDOW w_recent AS (ORDER BY running_product_id_max_units_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Grouped metrics
SELECT
    CASE WHEN first_known_last_known_row_seq_times_units_avg_3 >= 85 THEN 'PREMIUM' ELSE 'STANDARD' END AS first_known_last_known_row_seq_times_units_avg_3_tier,
    CASE WHEN row_seq_percent_rank_int >= 76 THEN 'PREMIUM' ELSE 'STANDARD' END AS row_seq_percent_rank_int_tier,
    ROUND(AVG(units_avg_3_avg_3_bucket), 2) AS avg_units_avg_3_avg_3_bucket,
    PERCENTILE_APPROX(product_id_max_units_share, ARRAY(0.25, 0.5, 0.75)) AS product_id_max_units_share_quartiles,
    APPROX_PERCENTILE(running_units_avg_3_avg_3, ARRAY(0.25, 0.5, 0.75)) AS running_units_avg_3_avg_3_quartiles,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(DISTINCT id) AS distinct_ids_2,
    COUNT(*) AS cnt,
    DENSE_RANK() OVER (PARTITION BY CASE WHEN first_known_last_known_row_seq_times_units_avg_3 >= 85 THEN 'PREMIUM' ELSE 'STANDARD' END ORDER BY COUNT(DISTINCT id) DESC) AS distinct_ids_dense_rank
FROM enriched_store_sales
GROUP BY CASE WHEN first_known_last_known_row_seq_times_units_avg_3 >= 85 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN row_seq_percent_rank_int >= 76 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY distinct_ids_2 DESC
