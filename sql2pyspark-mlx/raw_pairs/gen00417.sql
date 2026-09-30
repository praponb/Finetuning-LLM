WITH lines AS (
    SELECT
        id,
        plan,
        effective_from,
        EXPLODE(discounts) AS tier_value
    FROM price_tiers
)
SELECT
    plan,
    effective_from,
    CASE WHEN plan RLIKE '^(pro|enterprise)$' THEN 'PRIORITY' WHEN plan RLIKE '^(starter|basic)$' THEN 'CORE' ELSE 'GROUP_B' END AS plan_zone
FROM lines;
