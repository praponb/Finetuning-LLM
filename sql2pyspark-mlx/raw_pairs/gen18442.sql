WITH lines AS (
    SELECT
        id,
        customer_id,
        created_at,
        key,
        value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) li AS key, value
)
SELECT
    s.id,
    s.customer_id,
    s.key,
    s.value,
    CASE WHEN key RLIKE '^(weight_kg|utm_source)$' THEN 'PRIORITY' WHEN key RLIKE '^(device|gift)$' THEN 'OTHER_ZONE' ELSE 'EXTENDED' END AS key_zone,
    (SELECT SUM(s2.monthly_fee) FROM subscriptions s2 WHERE s2.customer_id = s.customer_id) AS sum_subscription_monthly_fee
FROM lines s
ORDER BY id DESC NULLS LAST;
