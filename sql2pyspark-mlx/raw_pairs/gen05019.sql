WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_limits) x AS val
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, INITCAP(TRIM(currency)) AS currency_title
    FROM exploded
    WHERE currency = 'THB'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY val DESC NULLS LAST) AS val_cume_dist,
        PERCENT_RANK() OVER (ORDER BY val DESC NULLS FIRST) AS val_percent_rank
    FROM filtered
    WHERE currency = 'THB'
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        currency,
        val,
        currency_title,
        val_percent_rank,
        MIN(val) OVER (PARTITION BY currency) AS currency_min_val
    FROM with_windows
)
-- Aggregate with conditional metrics
SELECT
    currency,
    AVG(currency_min_val) AS avg_currency_min_val,
    ROUND(SUM(currency_min_val) / NULLIF(COUNT(CASE WHEN currency = 'EUR' THEN 1 END), 0), 2) AS currency_min_val_per_eur,
    PERCENTILE_APPROX(val_percent_rank, 0.9) AS val_percent_rank_p90
FROM metrics
GROUP BY currency
ORDER BY currency_min_val_per_eur
