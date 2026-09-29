WITH filtered AS (
    SELECT *, DATEDIFF(CURRENT_DATE(), start_date) / 365 AS years_since_start_date
    FROM subscriptions
    WHERE end_date > DATE '2023-09-05'
      AND customer_id IS NOT NULL
)
SELECT
    id,
    start_date,
    end_date,
    monthly_fee,
    years_since_start_date,
    MIN(years_since_start_date) OVER w_part AS last7_min_years_since_start_date,
    MAX(years_since_start_date) OVER w_part AS last7_max_years_since_start_date,
    SUM(years_since_start_date) OVER w_part AS last7_sum_years_since_start_date,
    NTILE(5) OVER (PARTITION BY status ORDER BY monthly_fee DESC, id) AS monthly_fee_bucket
FROM filtered
WINDOW w_part AS (PARTITION BY plan ORDER BY monthly_fee, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
