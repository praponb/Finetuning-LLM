WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        FLOOR(premium / 1000) * 1000 AS premium_bucket
    FROM policies
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        premium_bucket,
        ROUND(premium - AVG(premium) OVER w_holder_id_end_date_month, 2) AS premium_vs_holder_id_end_date_month_avg,
        LAG(premium, 1, 0) OVER w_end_date_year_by_end_date AS prev_premium
    FROM base
    WINDOW w_holder_id_end_date_month AS (PARTITION BY holder_id, MONTH(end_date)), w_end_date_year_by_end_date AS (PARTITION BY YEAR(end_date) ORDER BY end_date DESC NULLS FIRST, id)
),
with_windows_2 AS (
    -- Window metrics over several partitions
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        premium_bucket,
        prev_premium,
        FIRST_VALUE(premium_bucket) OVER (ORDER BY premium_vs_holder_id_end_date_month_avg ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_premium_bucket,
        ROUND(PERCENT_RANK() OVER (ORDER BY premium_vs_holder_id_end_date_month_avg ASC, id), 3) AS premium_vs_holder_id_end_date_month_avg_percent_
    FROM with_windows
)
SELECT
    id,
    holder_id,
    premium,
    end_date,
    premium_bucket,
    prev_premium,
    first_premium_bucket,
    premium_vs_holder_id_end_date_month_avg_percent_,
    ABS(first_premium_bucket - 3560) AS first_premium_bucket_distance
FROM with_windows_2
WHERE premium_vs_holder_id_end_date_month_avg_percent_ >= 0.3
ORDER BY premium DESC, id
LIMIT 20
