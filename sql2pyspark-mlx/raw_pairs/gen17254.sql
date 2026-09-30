WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        units * revenue AS revenue_per_unit,
        CASE WHEN revenue >= 3100 THEN 'High' WHEN revenue >= 800 THEN 'Medium' ELSE 'Low' END AS revenue_band
    FROM store_sales
    WHERE sale_date > DATE '2023-09-21'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(units) OVER w_grp AS last4_sum_units,
        MIN(units) OVER w_grp AS last4_min_units,
        COUNT(units) OVER w_grp AS last4_count_units
    FROM filtered
    WINDOW w_grp AS (PARTITION BY store_id, product_id ORDER BY sale_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
filtered_store_sales AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        units,
        revenue,
        revenue_per_unit,
        revenue_band,
        ROUND(last4_count_units * 1.15, 2) AS adjusted_last4_count_units,
        CAST(units AS STRING) AS units_num
    FROM scored
    WHERE sale_date >= DATE '2023-03-15'
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(adjusted_last4_count_units) OVER w_hist AS cumulative_adjusted_last4_count_units,
        ROW_NUMBER() OVER (ORDER BY units, id) AS seq_no,
        DENSE_RANK() OVER (ORDER BY units DESC NULLS LAST) AS units_dense_rank
    FROM filtered_store_sales
    WINDOW w_hist AS (ORDER BY units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        revenue,
        revenue_band,
        adjusted_last4_count_units,
        units_num,
        cumulative_adjusted_last4_count_units,
        seq_no,
        units_dense_rank,
        cumulative_adjusted_last4_count_units * seq_no AS cumulative_adjusted_last4_count_units_times_seq_no
    FROM metrics
    WHERE revenue >= 3100
),
scored_store_sales AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(revenue) OVER w_running AS next_revenue,
        LEAD(units_dense_rank) OVER w_running AS next_units_dense_rank,
        LAG(units_dense_rank) OVER w_running AS prev_units_dense_rank,
        ROW_NUMBER() OVER w_running AS row_seq
    FROM cleaned
    WINDOW w_running AS (ORDER BY units_dense_rank, id)
),
cleaned_store_sales AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        revenue_band,
        units_num,
        cumulative_adjusted_last4_count_units,
        cumulative_adjusted_last4_count_units_times_seq_no,
        next_revenue,
        prev_units_dense_rank,
        row_seq,
        revenue + adjusted_last4_count_units AS revenue_plus_adjusted_last4_count_units
    FROM scored_store_sales
    WHERE row_seq BETWEEN 4 AND 6
)
-- Multi-level aggregation
SELECT
    CASE WHEN prev_units_dense_rank >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS prev_units_dense_rank_tier,
    APPROX_PERCENTILE(row_seq, 0.9) AS row_seq_p90,
    PERCENTILE_APPROX(prev_units_dense_rank, ARRAY(0.25, 0.5, 0.75)) AS prev_units_dense_rank_quartiles,
    COUNT(DISTINCT id) AS distinct_ids,
    APPROX_PERCENTILE(prev_units_dense_rank, ARRAY(0.25, 0.5, 0.75)) AS prev_units_dense_rank_quartiles_2
FROM cleaned_store_sales
GROUP BY prev_units_dense_rank_tier;
