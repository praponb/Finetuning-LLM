WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        monthly_fee,
        status,
        ADD_MONTHS(start_date, 6) AS start_date_plus_months
    FROM subscriptions
    WHERE plan = 'premium'
      AND customer_id IS NOT NULL
)
-- Running and moving aggregates
SELECT
    *,
    ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY plan), 4) AS monthly_fee_share,
    LAG(monthly_fee, 2, 0) OVER w_hist AS prev_monthly_fee
FROM filtered
WINDOW w_hist AS (PARTITION BY status, customer_id ORDER BY start_date_plus_months, id)
