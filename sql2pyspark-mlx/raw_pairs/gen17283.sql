WITH exploded AS (
    SELECT
        id,
        plan,
        val
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_limits) e AS val
)
SELECT
    id,
    plan,
    val,
    CASE WHEN plan RLIKE '^(enterprise|starter)$' THEN 'PRIORITY' WHEN plan RLIKE '^(basic|pro)$' THEN 'GROUP_B' ELSE 'EXTENDED' END AS plan_zone
FROM exploded
ORDER BY id DESC
