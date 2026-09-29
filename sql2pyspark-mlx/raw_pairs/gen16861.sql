WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_prices) t AS val
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), effective_from) / 30 months_since_effective_from,
        LENGTH(plan) plan_length
    FROM exploded
    WHERE effective_from > DATE '2024-03-21'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        plan,
        effective_from,
        months_since_effective_from,
        RANK() OVER (PARTITION BY currency ORDER BY val DESC) val_rank,
        STDDEV_SAMP(months_since_effective_from) OVER w_hist currency_stddev_samp_months_since_effective_from,
        SUM(val) OVER w_hist currency_sum_val
    FROM valid
    WINDOW w_hist AS (PARTITION BY currency)
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        plan,
        val_rank,
        currency_stddev_samp_months_since_effective_from,
        COALESCE(currency_sum_val, 0) currency_sum_val_filled,
        currency_stddev_samp_months_since_effective_from - months_since_effective_from currency_stddev_samp_months_since_effective_from_minus_months_since_effective_from
    FROM metrics
    WHERE currency_sum_val >= 327
),
matched_keys AS (
    -- Set difference on keys
    SELECT plan, id
    FROM filtered
    EXCEPT
    SELECT plan, id
    FROM price_tiers
    WHERE plan = 'starter'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN matched_keys k ON s.plan = k.plan AND s.id = k.id
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CAST(currency_stddev_samp_months_since_effective_from AS STRING) currency_stddev_samp_months_since_effective_from_num,
        LOWER(plan) plan_lower
    FROM reconciled
    WHERE currency_stddev_samp_months_since_effective_from BETWEEN 61 AND 66
)
-- Grouped metrics
SELECT
    plan,
    COUNT_IF(plan = 'starter') AS starter_events,
    COUNT(*) FILTER (WHERE plan = 'starter') AS starter_rows,
    MAX(val_rank) FILTER (WHERE plan = 'starter') AS starter_max_val_rank
FROM base
GROUP BY plan
ORDER BY starter_max_val_rank DESC
