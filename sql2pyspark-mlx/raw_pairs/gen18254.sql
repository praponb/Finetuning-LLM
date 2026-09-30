SELECT
    id,
    monthly_fee,
    plan,
    status,
    CASE WHEN status IN ('ACTIVE', 'CANCELLED') THEN 'Priority' ELSE 'Other' END status_group
FROM subscriptions
ORDER BY plan;
