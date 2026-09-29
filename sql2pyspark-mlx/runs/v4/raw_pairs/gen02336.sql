WITH lines AS (
    SELECT
        id,
        plan,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_limits) t AS val
)
SELECT
    plan,
    currency,
    SORT_ARRAY(COLLECT_SET(currency)) AS currency_list,
    COUNT_IF(currency = 'USD') AS usd_events,
    PERCENTILE_APPROX(val, 0.9) AS val_p90,
    SUM(CASE WHEN plan = 'enterprise' THEN val ELSE 0 END) AS enterprise_val,
    APPROX_PERCENTILE(val, 0.95) AS val_p95
FROM lines
GROUP BY plan, currency
