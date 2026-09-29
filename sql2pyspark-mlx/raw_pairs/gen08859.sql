WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_prices) t AS val
)
-- Ranking and gap features
SELECT
    effective_from,
    currency,
    val,
    PERCENT_RANK() OVER (PARTITION BY currency ORDER BY val NULLS LAST) AS val_percent_rank,
    MIN(val) OVER w_hist AS effective_from_period_min_val
FROM lines
WINDOW w_hist AS (PARTITION BY DATE_TRUNC('YEAR', effective_from), currency)
