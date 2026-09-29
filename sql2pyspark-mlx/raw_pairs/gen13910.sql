WITH base AS (
    SELECT *, DATEDIFF(CURRENT_DATE(), end_date) / 365 years_since_end_date
    FROM subscriptions
    WHERE start_date >= TO_DATE('2023-09-15')
      AND customer_id IS NOT NULL
),
ranked AS (
    SELECT
        id,
        plan,
        start_date,
        end_date,
        status,
        years_since_end_date
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY plan ORDER BY monthly_fee DESC NULLS LAST, id) = 1
),
with_windows AS (
    SELECT
        id,
        start_date,
        status,
        NTILE(4) OVER (PARTITION BY MONTH(end_date), status ORDER BY years_since_end_date DESC, id) years_since_end_date_bucket,
        LAG(years_since_end_date, 1) OVER (PARTITION BY plan ORDER BY end_date, id) prev_years_since_end_date
    FROM ranked
),
enriched AS (
    SELECT
        id,
        start_date,
        status,
        prev_years_since_end_date,
        NTILE(3) OVER (PARTITION BY YEAR(start_date), status ORDER BY prev_years_since_end_date DESC, id) prev_years_since_end_date_bucket,
        AVG(years_since_end_date_bucket) OVER (PARTITION BY status ORDER BY years_since_end_date_bucket, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) years_since_end_date_bucket_avg_6
    FROM with_windows
)
SELECT
    CASE WHEN years_since_end_date_bucket_avg_6 >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END AS years_since_end_date_bucket_avg_6_tier,
    status,
    DATE_TRUNC('WEEK', start_date) AS week_start,
    COUNT(CASE WHEN status = 'PAUSED' THEN 1 END) AS paused_count,
    COUNT(CASE WHEN status = 'PAUSED' THEN 1 END) AS paused_count_2,
    ROUND(SUM(prev_years_since_end_date) / NULLIF(COUNT(CASE WHEN status = 'CANCELLED' THEN 1 END), 0), 2) AS prev_years_since_end_date_per_cancelled,
    PERCENT_RANK() OVER (PARTITION BY CASE WHEN years_since_end_date_bucket_avg_6 >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END ORDER BY COUNT(CASE WHEN status = 'PAUSED' THEN 1 END) DESC) AS paused_count_2_percent_rank
FROM enriched
GROUP BY years_since_end_date_bucket_avg_6_tier, status, week_start
HAVING prev_years_since_end_date_per_cancelled > 0
ORDER BY status NULLS LAST
