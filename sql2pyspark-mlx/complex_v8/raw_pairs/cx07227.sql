WITH valid_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        CONCAT_WS('-', status, product) AS status_product_label,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown,
        COALESCE(product, 'UNKNOWN') AS product_or_unknown
    FROM policies
    WHERE status = 'ACTIVE'
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', start_date) AS start_date_month,
        status,
        PERCENTILE_APPROX(premium, 0.5) AS median_premium,
        COUNT(*) AS row_count,
        DATEDIFF(MAX(end_date), MIN(end_date)) AS end_date_span_days,
        MIN(premium) AS min_premium
    FROM valid_policys
    GROUP BY DATE_TRUNC('MONTH', start_date), status
    HAVING COUNT(*) >= 2
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        ABS(min_premium - 2120.0) AS min_premium_distance,
        ROUND(end_date_span_days / NULLIF(min_premium, 0), 3) AS end_date_span_days_per_min_premium,
        LEAST(end_date_span_days, median_premium) AS least_end_date_span_days_median_premium
    FROM per_group
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAG(min_premium) OVER w_status_by_end_date_span_days AS prev_min_premium,
        SUM(min_premium) OVER (PARTITION BY status, row_count) AS status_row_count_sum_min_premium
    FROM labeled
    WINDOW w_status_by_end_date_span_days AS (PARTITION BY status ORDER BY end_date_span_days DESC, start_date_month, status)
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(row_count, 3) OVER (PARTITION BY row_count, status ORDER BY end_date_span_days ASC, start_date_month, status) AS prev3_row_count,
        ROUND(PERCENT_RANK() OVER (PARTITION BY row_count, status ORDER BY end_date_span_days ASC, start_date_month, status), 3) AS end_date_span_days_percent_rank,
        NTILE(5) OVER w_row_count_status_by_end_date AS end_date_span_days_quintile
    FROM with_trends
    WINDOW w_row_count_status_by_end_date AS (PARTITION BY row_count, status ORDER BY end_date_span_days ASC, start_date_month, status)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(min_premium_distance, 1, 0) OVER (ORDER BY prev_min_premium DESC, start_date_month, status) AS prev_min_premium_distance,
        SIZE(COLLECT_SET(status) OVER w_end_date_span_days_quintile) AS distinct_status_in_end_date_span_days_quintile_s,
        DENSE_RANK() OVER (PARTITION BY row_count, status ORDER BY end_date_span_days_per_min_premium ASC) AS end_date_span_days_per_min_premium_dense_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY row_count, status ORDER BY end_date_span_days_per_min_premium ASC), 3) AS end_date_span_days_per_min_premium_percent_rank,
        MAX(end_date_span_days_quintile) OVER w_end_date_span_days_quintile AS end_date_span_days_quintile_start_date_month_mon
    FROM windowed
    WINDOW w_end_date_span_days_quintile AS (PARTITION BY end_date_span_days_quintile, MONTH(start_date_month))
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM metrics
    WHERE row_count > (SELECT AVG(row_count) FROM metrics)
)
SELECT
    start_date_month,
    status,
    median_premium,
    row_count,
    end_date_span_days,
    min_premium,
    min_premium_distance,
    end_date_span_days_per_min_premium,
    prev_min_premium,
    status_row_count_sum_min_premium,
    prev3_row_count,
    prev_min_premium_distance,
    distinct_status_in_end_date_span_days_quintile_s,
    end_date_span_days_per_min_premium_dense_rank,
    end_date_span_days_per_min_premium_percent_rank,
    end_date_span_days_quintile_start_date_month_mon,
    LOWER(TRIM(status)) AS status_clean
FROM with_activity
