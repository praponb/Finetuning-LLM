WITH ranked AS (
    SELECT *
    FROM subscriptions
    QUALIFY RANK() OVER (PARTITION BY customer_id ORDER BY monthly_fee) = 1
)
SELECT
    customer_id,
    status,
    STDDEV_SAMP(monthly_fee) AS monthly_fee_stddev,
    STDDEV_SAMP(monthly_fee) AS monthly_fee_stddev_2,
    SUM(CASE WHEN plan = 'basic' THEN monthly_fee ELSE 0 END) AS basic_monthly_fee,
    COUNT_IF(plan = 'enterprise') AS enterprise_events,
    COUNT(CASE WHEN status = 'EXPIRED' THEN 1 END) AS expired_count,
    RANK() OVER (PARTITION BY customer_id ORDER BY STDDEV_SAMP(monthly_fee) DESC) AS monthly_fee_stddev_rank
FROM ranked
GROUP BY customer_id, status;
