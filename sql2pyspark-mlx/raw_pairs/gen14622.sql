WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW EXPLODE(discounts) e AS tier_value
),
kept_keys AS (
    -- Set intersection on keys
    SELECT plan, currency
    FROM flattened
    INTERSECT
    SELECT plan, currency
    FROM price_tiers
    WHERE plan != 'starter'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM flattened s
    INNER JOIN kept_keys k ON s.plan = k.plan AND s.currency = k.currency
),
remaining_keys AS (
    -- Set difference on keys
    SELECT currency
    FROM kept_rows
    EXCEPT
    SELECT currency
    FROM price_tiers
    WHERE effective_from BETWEEN '2023-05-01' AND '2023-12-25'
),
kept_rows_price_tiers AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM kept_rows s
    INNER JOIN remaining_keys k ON s.currency = k.currency
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, UPPER(currency) AS currency_upper
    FROM kept_rows_price_tiers
    WHERE effective_from >= TO_DATE('2024-02-24')
      AND tier_value > 31
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (PARTITION BY YEAR(effective_from), plan ORDER BY tier_value DESC NULLS LAST) AS tier_value_rank,
        SUM(tier_value) OVER w_part AS plan_sum_tier_value
    FROM cleaned
    WINDOW w_part AS (PARTITION BY plan, currency)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        plan,
        PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('WEEK', effective_from), plan ORDER BY plan_sum_tier_value DESC NULLS LAST) AS plan_sum_tier_value_percent_rank,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY tier_value_rank ASC) AS tier_value_rank_dense_rank,
        STDDEV_SAMP(tier_value_rank) OVER (PARTITION BY MONTH(effective_from), plan) AS effective_from_month_stddev_samp_tier_value_rank,
        CUME_DIST() OVER (PARTITION BY MONTH(effective_from) ORDER BY tier_value ASC NULLS FIRST) AS tier_value_cume_dist,
        COUNT(tier_value) OVER (PARTITION BY DATE_TRUNC('WEEK', effective_from)) AS effective_from_period_count_tier_value
    FROM scored
    WHERE plan_sum_tier_value != 380
)
SELECT
    plan,
    plan_sum_tier_value_percent_rank,
    effective_from_month_stddev_samp_tier_value_rank
FROM enriched
