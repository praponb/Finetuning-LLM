WITH cleaned_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM policies
    WHERE status IS NOT NULL
      AND premium > 1051.9
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(premium / NULLIF(SUM(premium) OVER w_holder_id_start_date_month, 0), 4) AS premium_share_of_holder_id_start_date_month,
        SIZE(COLLECT_SET(product) OVER w_holder_id) AS distinct_product_in_holder_id,
        NTH_VALUE(end_date, 2) OVER (PARTITION BY holder_id ORDER BY start_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_end_date,
        COUNT(*) OVER (PARTITION BY holder_id, MONTH(start_date)) AS rows_in_holder_id_start_date_month,
        DATEDIFF(start_date, LAG(start_date) OVER (PARTITION BY holder_id, product ORDER BY end_date, id)) AS days_since_prev_start_date,
        SUM(premium) OVER (PARTITION BY product, holder_id ORDER BY premium, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_premium,
        ROUND(premium - AVG(premium) OVER (PARTITION BY MONTH(start_date), product), 2) AS premium_vs_start_date_month_product_avg,
        ROUND(AVG(premium) OVER (PARTITION BY product, holder_id ORDER BY premium, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS premium_avg_3_prior,
        ROUND(AVG(premium) OVER (PARTITION BY status, product ORDER BY start_date DESC NULLS FIRST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS premium_avg_4
    FROM cleaned_policys
    WINDOW w_holder_id_start_date_month AS (PARTITION BY holder_id, MONTH(start_date)), w_holder_id AS (PARTITION BY holder_id)
),
derived AS (
    -- Add calculated columns
    SELECT *, DATE_FORMAT(end_date, 'yyyy-MM') AS end_date_label
    FROM with_windows
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        YEAR(start_date) AS start_date_year,
        COUNT(*) AS cnt,
        COUNT(DISTINCT holder_id) AS distinct_holder_ids,
        PERCENTILE_APPROX(premium_vs_start_date_month_product_avg, 0.5) AS median_premium_vs_start_date_month_product_avg,
        ROUND(SUM(premium_avg_4), 2) AS total_premium_avg_4,
        MIN(running_premium) AS min_running_premium
    FROM derived
    GROUP BY YEAR(start_date)
)
SELECT
    start_date_year,
    distinct_holder_ids,
    median_premium_vs_start_date_month_product_avg
FROM stats
ORDER BY distinct_holder_ids DESC, median_premium_vs_start_date_month_product_avg DESC, start_date_year
LIMIT 5
