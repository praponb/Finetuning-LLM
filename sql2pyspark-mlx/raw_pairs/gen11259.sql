WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_limits) li AS val
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(val * 100, 2) AS val_scaled,
        SUBSTRING(currency, 1, 4) AS currency_prefix
    FROM lines
    WHERE currency = 'EUR'
),
matched_keys AS (
    -- Set difference on keys
    SELECT id, currency
    FROM filtered
    EXCEPT
    SELECT id, currency
    FROM price_tiers
    WHERE currency = 'EUR'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN matched_keys k ON s.id = k.id AND s.currency = k.currency
),
kept_keys AS (
    -- Set difference on keys
    SELECT plan, id
    FROM kept_rows
    EXCEPT
    SELECT plan, id
    FROM price_tiers
    WHERE effective_from > '2023-08-17'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM kept_rows s
    INNER JOIN kept_keys k ON s.plan = k.plan AND s.id = k.id
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN val >= 64 THEN 'HIGH' ELSE 'LOW' END AS val_tier,
    APPROX_PERCENTILE(val_scaled, ARRAY(0.25, 0.5, 0.75)) AS val_scaled_quartiles,
    APPROX_PERCENTILE(val_scaled, 0.9) AS val_scaled_p90,
    SORT_ARRAY(COLLECT_SET(currency)) AS currency_list,
    SIZE(COLLECT_SET(plan)) AS num_plans,
    ROUND(SUM(val) / NULLIF(COUNT(CASE WHEN plan = 'enterprise' THEN 1 END), 0), 2) AS val_per_enterprise,
    PERCENTILE_APPROX(val, ARRAY(0.25, 0.5, 0.75)) AS val_quartiles
FROM verified
GROUP BY CASE WHEN val >= 64 THEN 'HIGH' ELSE 'LOW' END
HAVING APPROX_PERCENTILE(val_scaled, 0.9) > 0
ORDER BY val_per_enterprise DESC
