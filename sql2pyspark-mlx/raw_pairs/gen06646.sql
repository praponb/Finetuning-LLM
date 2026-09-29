WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_prices) e AS val
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        plan,
        effective_from,
        currency,
        val,
        FLOOR(val) AS val_whole
    FROM flattened
    WHERE effective_from >= DATE '2024-05-04'
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY val_whole DESC NULLS FIRST) AS val_whole_percent_rank,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY val) AS val_percent_rank
    FROM base
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        plan,
        effective_from,
        currency,
        val,
        val_whole,
        val_whole_percent_rank,
        val_percent_rank,
        STDDEV_SAMP(val) OVER (PARTITION BY MONTH(effective_from)) AS effective_from_month_stddev_samp_val
    FROM scored
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT currency
    FROM metrics
    INTERSECT ALL
    SELECT currency
    FROM price_tiers
    WHERE currency = 'USD'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    INNER JOIN remaining_keys k ON s.currency = k.currency
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        plan,
        effective_from,
        val_whole,
        val_whole_percent_rank,
        effective_from_month_stddev_samp_val,
        SUM(val_whole) OVER (PARTITION BY plan) AS plan_sum_val_whole
    FROM reconciled
    WHERE effective_from_month_stddev_samp_val BETWEEN 56 AND 63
),
base_price_tiers AS (
    -- Basic cleansing and defaults
    SELECT *, COALESCE(val_whole, 0) AS val_whole_filled
    FROM with_windows
    WHERE effective_from >= TO_DATE('2024-02-02')
      AND val_whole_percent_rank BETWEEN 0.55 AND 0.62
)
SELECT
    plan,
    effective_from,
    val_whole,
    val_whole_percent_rank,
    effective_from_month_stddev_samp_val,
    plan_sum_val_whole,
    CASE WHEN plan RLIKE '^(enterprise|basic)$' THEN 'OTHER_ZONE' ELSE 'PRIORITY' END AS plan_zone
FROM base_price_tiers
ORDER BY plan_zone DESC NULLS LAST
