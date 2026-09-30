WITH scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(monthly_fee) OVER (PARTITION BY status, plan ORDER BY start_date, id) AS next_monthly_fee,
        ROW_NUMBER() OVER (PARTITION BY status, plan ORDER BY start_date, id) AS seq_no
    FROM subscriptions
    WHERE monthly_fee < 70
)
SELECT
    s.id,
    s.customer_id,
    s.plan,
    s.start_date,
    s.end_date,
    s.monthly_fee,
    s.seq_no,
    CASE WHEN plan RLIKE '^(standard|enterprise)$' THEN 'OTHER_ZONE' WHEN plan RLIKE '^(basic|premium)$' THEN 'PRIORITY' ELSE 'CORE' END AS plan_zone,
    (SELECT SUM(x.monthly_fee) FROM subscriptions x WHERE x.customer_id = s.customer_id) AS sum_subscription_monthly_fee
FROM scored s
