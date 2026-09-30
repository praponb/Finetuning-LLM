WITH enriched AS (
    SELECT
        id,
        plan,
        end_date,
        status,
        ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY status, customer_id), 4) AS monthly_fee_share,
        STDDEV_SAMP(monthly_fee) OVER (PARTITION BY customer_id) AS customer_id_stddev_samp_monthly_fee
    FROM subscriptions
    WHERE status IN ('CANCELLED', 'PAUSED')
)
SELECT id, end_date
FROM enriched
ORDER BY id NULLS FIRST, end_date DESC
LIMIT 5;
