WITH lines AS (
    SELECT
        id,
        plan,
        currency,
        EXPLODE_OUTER(tier_limits) AS val
    FROM price_tiers
)
SELECT
    id,
    plan,
    currency,
    CASE WHEN plan RLIKE '^(enterprise|pro)$' THEN 'OTHER_ZONE' ELSE 'PRIORITY' END AS plan_zone
FROM lines
