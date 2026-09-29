WITH metrics AS (
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        AVG(monthly_fee) OVER (PARTITION BY customer_id, status) AS customer_id_avg_monthly_fee,
        CUME_DIST() OVER w_hist AS monthly_fee_cume_dist,
        LAST_VALUE(monthly_fee, TRUE) OVER w_grp AS last_known_monthly_fee
    FROM subscriptions
    WINDOW w_hist AS (PARTITION BY plan ORDER BY monthly_fee), w_grp AS (PARTITION BY plan ORDER BY monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    plan,
    start_date,
    end_date,
    monthly_fee,
    status,
    customer_id_avg_monthly_fee,
    monthly_fee_cume_dist,
    last_known_monthly_fee,
    CASE WHEN status RLIKE '^(EXPIRED|ACTIVE)$' THEN 'CORE' WHEN status RLIKE '^(CANCELLED|PAUSED)$' THEN 'PRIORITY' ELSE 'OTHER_ZONE' END AS status_zone
FROM metrics
ORDER BY plan DESC NULLS LAST
