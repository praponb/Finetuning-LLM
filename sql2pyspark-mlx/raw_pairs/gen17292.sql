WITH exploded AS (
    SELECT
        id,
        plan,
        effective_from,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_limits) e AS val
),
filtered AS (
    SELECT *, CONCAT_WS('-', currency, plan) AS currency_plan_key
    FROM exploded
    WHERE effective_from > DATE '2024-01-31'
),
with_windows AS (
    SELECT
        id,
        plan,
        effective_from,
        currency,
        COUNT(val) OVER (PARTITION BY plan, currency) AS plan_count_val,
        MIN(val) OVER (PARTITION BY currency, plan) AS currency_min_val
    FROM filtered
),
cleaned AS (
    SELECT *, SUBSTRING(currency, 1, 2) AS currency_prefix
    FROM with_windows
    WHERE effective_from <= '2023-03-01'
),
matched_keys AS (
    SELECT plan, id
    FROM cleaned
    EXCEPT ALL
    SELECT plan, id
    FROM price_tiers
    WHERE MONTH(effective_from) IN (4, 5, 6)
),
reconciled AS (
    SELECT s.*
    FROM cleaned s
    INNER JOIN matched_keys k ON s.plan = k.plan AND s.id = k.id
)
SELECT
    DATE_TRUNC('QUARTER', effective_from) AS quarter_start,
    PERCENTILE_APPROX(currency_min_val, 0.95) AS currency_min_val_p95,
    AVG(plan_count_val) FILTER (WHERE plan = 'pro') AS pro_avg_plan_count_val,
    COUNT(CASE WHEN plan = 'basic' THEN 1 END) AS basic_count,
    COUNT(*) FILTER (WHERE plan = 'enterprise') AS enterprise_rows,
    COUNT(*) FILTER (WHERE currency = 'EUR') AS eur_rows
FROM reconciled
GROUP BY DATE_TRUNC('QUARTER', effective_from)
ORDER BY enterprise_rows DESC NULLS LAST
