WITH with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        CUME_DIST() OVER w_recent AS monthly_fee_cume_dist,
        LAG(monthly_fee, 2) OVER (PARTITION BY customer_id, plan ORDER BY monthly_fee, id) AS prev_monthly_fee,
        AVG(monthly_fee) OVER (ORDER BY monthly_fee, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS monthly_fee_avg_3
    FROM subscriptions
    WHERE start_date > '2023-06-20'
    WINDOW w_recent AS (PARTITION BY DATE_TRUNC('month', start_date), status ORDER BY monthly_fee DESC NULLS FIRST)
)
SELECT
    id,
    plan,
    start_date,
    end_date,
    prev_monthly_fee
FROM with_windows
ORDER BY start_date DESC NULLS FIRST
