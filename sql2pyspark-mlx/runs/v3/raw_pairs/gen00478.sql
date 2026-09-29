SELECT
    id,
    customer_id,
    monthly_fee,
    plan,
    status,
    FIRST_VALUE(plan) OVER (PARTITION BY customer_id ORDER BY monthly_fee DESC, id) AS first_plan,
    COUNT(*) OVER (PARTITION BY status) AS status_count
FROM subscriptions;
