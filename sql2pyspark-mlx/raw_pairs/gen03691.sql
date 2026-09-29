WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW EXPLODE(discounts) li AS val
),
matched_keys AS (
    -- Set difference on keys
    SELECT id, plan
    FROM flattened
    EXCEPT
    SELECT id, plan
    FROM price_tiers
    WHERE currency = 'USD'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM flattened s
    INNER JOIN matched_keys k ON s.id = k.id AND s.plan = k.plan
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER w_ordered AS val_percent_rank,
        STDDEV_SAMP(val) OVER (PARTITION BY currency) AS currency_stddev_samp_val,
        MAX(val) OVER w_hist AS plan_max_val
    FROM reconciled
    WHERE val > 70
    WINDOW w_ordered AS (PARTITION BY plan ORDER BY val DESC NULLS FIRST), w_hist AS (PARTITION BY plan)
),
scored_price_tiers AS (
    -- Running and moving aggregates
    SELECT
        id,
        plan,
        val,
        val_percent_rank,
        plan_max_val,
        AVG(val_percent_rank) OVER (PARTITION BY currency) AS currency_avg_val_percent_rank
    FROM scored
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    plan,
    measure,
    val
FROM (
    SELECT
        plan,
        val_percent_rank,
        val
    FROM scored_price_tiers
) src
UNPIVOT (
    val FOR measure IN (val_percent_rank AS VAL_PERCENT_RANK_VAL, val AS VAL_VAL)
)
