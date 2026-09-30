WITH cleaned AS (
    SELECT *, CASE WHEN plan IN ('premium', 'enterprise') THEN 'Priority' ELSE 'Other' END AS plan_group
    FROM subscriptions
    WHERE customer_id IS NOT NULL
)
SELECT
    *,
    FIRST_VALUE(monthly_fee) IGNORE NULLS OVER (PARTITION BY status ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_monthly_fee,
    LAST_VALUE(monthly_fee) IGNORE NULLS OVER (PARTITION BY plan ORDER BY monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_monthly_fee,
    CUME_DIST() OVER (ORDER BY monthly_fee DESC) AS monthly_fee_cume_dist
FROM cleaned
