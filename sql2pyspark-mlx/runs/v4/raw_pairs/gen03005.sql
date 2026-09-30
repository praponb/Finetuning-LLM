WITH lines AS (
    SELECT
        id,
        plan,
        effective_from,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(discounts) li AS val
),
metrics AS (
    SELECT
        *,
        MIN(val) OVER (PARTITION BY plan, currency) AS plan_min_val,
        AVG(val) OVER (PARTITION BY currency) AS currency_avg_val
    FROM lines
),
filtered AS (
    SELECT
        *,
        CONCAT(plan, ' - ', currency) AS plan_currency,
        CONCAT_WS(',', plan, currency) AS plan_currency_key
    FROM metrics
    WHERE plan_min_val < 52
),
with_windows AS (
    SELECT
        id,
        plan,
        effective_from,
        currency,
        val,
        plan_min_val,
        currency_avg_val,
        plan_currency,
        plan_currency_key,
        MAX(val) OVER w_running AS plan_max_val,
        MAX(currency_avg_val) OVER w_running AS plan_max_currency_avg_val
    FROM filtered
    WINDOW w_running AS (PARTITION BY plan)
),
remaining_keys AS (
    SELECT plan, id
    FROM with_windows
    INTERSECT
    SELECT plan, id
    FROM price_tiers
    WHERE effective_from >= '2025-09-29'
),
kept_rows AS (
    SELECT s.*
    FROM with_windows s
    INNER JOIN remaining_keys k ON s.plan = k.plan AND s.id = k.id
),
scored AS (
    SELECT
        *,
        PERCENT_RANK() OVER w_recent AS val_percent_rank,
        PERCENT_RANK() OVER (ORDER BY val NULLS FIRST) AS val_percent_rank_2
    FROM kept_rows
    WINDOW w_recent AS (PARTITION BY plan ORDER BY val DESC NULLS LAST)
),
enriched AS (
    SELECT
        *,
        STDDEV_SAMP(val_percent_rank) OVER (PARTITION BY plan) AS plan_stddev_samp_val_percent_rank,
        MAX(plan_max_val) OVER (PARTITION BY currency, plan) AS currency_max_plan_max_val,
        COUNT(val_percent_rank) OVER (PARTITION BY plan, currency) AS plan_count_val_percent_rank
    FROM scored
)
SELECT
    id,
    plan,
    currency,
    val,
    currency_avg_val,
    plan_currency,
    plan_max_currency_avg_val,
    val_percent_rank,
    val_percent_rank_2
FROM enriched;
