WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATEDIFF(start_date, end_date) / 7 AS weeks_end_date_to_start_date,
        ROUND(monthly_fee * 1.1, 2) AS monthly_fee_adjusted
    FROM subscriptions
    WHERE end_date >= DATE '2023-10-23'
      AND MONTH(start_date) IN (2, 7, 11)
)
-- Ranking and gap features
SELECT
    *,
    LEAD(monthly_fee, 1) OVER (ORDER BY monthly_fee, id) AS next_monthly_fee,
    LAG(monthly_fee, 1) OVER (PARTITION BY plan ORDER BY weeks_end_date_to_start_date, id) AS prev_monthly_fee,
    NTILE(5) OVER (PARTITION BY status, plan ORDER BY weeks_end_date_to_start_date, id) AS weeks_end_date_to_start_date_bucket
FROM filtered
