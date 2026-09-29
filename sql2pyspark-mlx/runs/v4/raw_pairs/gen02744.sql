WITH flattened AS (
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
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        MIN(val) OVER (PARTITION BY currency) AS currency_min_val,
        PERCENT_RANK() OVER (ORDER BY val NULLS FIRST) AS val_percent_rank,
        RANK() OVER (PARTITION BY currency, plan ORDER BY val DESC NULLS LAST) AS val_rank
    FROM flattened
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        effective_from,
        currency,
        val,
        currency_min_val,
        val_percent_rank,
        val_rank,
        COALESCE(val_percent_rank, 0) AS val_percent_rank_filled,
        CASE WHEN val >= 75 THEN 'High' WHEN val >= 28 THEN 'Medium' ELSE 'Low' END AS val_band
    FROM scored
    WHERE effective_from >= DATE '2024-05-11'
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MIN(val_percent_rank) OVER (PARTITION BY currency) AS currency_min_val_percent_rank,
        ROUND(val_rank / SUM(val_rank) OVER (PARTITION BY currency), 4) AS val_rank_share,
        COUNT(val_percent_rank_filled) OVER (PARTITION BY currency) AS currency_count_val_percent_rank_filled,
        RANK() OVER (ORDER BY val_rank NULLS FIRST) AS val_rank_rank
    FROM base
)
-- Window metrics over several different partitions
SELECT
    *,
    MAX(currency_min_val_percent_rank) OVER w_running AS currency_max_currency_min_val_percent_rank,
    SUM(val_percent_rank) OVER w_running AS currency_sum_val_percent_rank,
    ROUND(val_rank_rank / SUM(val_rank_rank) OVER (PARTITION BY currency), 4) AS val_rank_rank_share,
    STDDEV_SAMP(val_percent_rank_filled) OVER w_running AS currency_stddev_samp_val_percent_rank_filled
FROM with_windows
WINDOW w_running AS (PARTITION BY currency);
