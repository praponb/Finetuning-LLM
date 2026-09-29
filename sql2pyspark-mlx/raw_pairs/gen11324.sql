WITH exploded AS (
    SELECT
        id,
        plan,
        effective_from,
        EXPLODE_OUTER(tier_limits) AS tier_value
    FROM price_tiers
),
cleaned AS (
    SELECT
        *,
        LAST_DAY(effective_from) AS effective_from_month_end,
        INITCAP(TRIM(plan)) AS plan_title
    FROM exploded
    WHERE effective_from < '2024-04-13'
),
matched_keys AS (
    SELECT id, plan
    FROM cleaned
    EXCEPT ALL
    SELECT id, plan
    FROM price_tiers
    WHERE plan = 'pro'
),
reconciled AS (
    SELECT s.*
    FROM cleaned s
    INNER JOIN matched_keys k ON s.id = k.id AND s.plan = k.plan
),
enriched AS (
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY tier_value DESC NULLS LAST) AS tier_value_percent_rank,
        AVG(tier_value) OVER (PARTITION BY plan) AS plan_avg_tier_value,
        DENSE_RANK() OVER w_ordered AS tier_value_dense_rank
    FROM reconciled
    WHERE plan = 'pro'
    WINDOW w_ordered AS (ORDER BY tier_value)
),
cleaned_price_tiers AS (
    SELECT
        *,
        DATE_TRUNC('MONTH', effective_from) AS effective_from_period,
        LOWER(plan) AS plan_lower
    FROM enriched
    WHERE tier_value_percent_rank <= 0.57
)
SELECT
    id,
    plan,
    effective_from,
    tier_value,
    plan_title,
    tier_value_percent_rank,
    plan_avg_tier_value,
    effective_from_period,
    plan_lower
FROM cleaned_price_tiers
ORDER BY effective_from_period DESC NULLS FIRST
