WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        DATEDIFF(CURRENT_DATE(), start_date) AS days_since_start_date
    FROM subscriptions
    WHERE customer_id IS NOT NULL
)
-- QUALIFY-style window filter
SELECT *
FROM filtered
QUALIFY RANK() OVER (PARTITION BY customer_id ORDER BY monthly_fee DESC) = 1
