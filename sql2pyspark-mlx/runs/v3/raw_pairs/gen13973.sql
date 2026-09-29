SELECT
    id,
    end_date,
    monthly_fee,
    plan,
    INITCAP(TRIM(plan)) AS plan_title
FROM subscriptions
WHERE status IN ('ACTIVE', 'EXPIRED', 'CANCELLED')
