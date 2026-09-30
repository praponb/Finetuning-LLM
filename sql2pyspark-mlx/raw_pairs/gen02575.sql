WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_prices) t AS val
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        TRIM(currency) AS clean_currency,
        DATE_ADD(effective_from, 66) AS effective_from_plus_66d
    FROM flattened
    WHERE effective_from > DATE '2024-05-17'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        CUME_DIST() OVER w_grp AS val_cume_dist,
        CUME_DIST() OVER w_grp AS val_cume_dist_2,
        AVG(val) OVER (PARTITION BY currency) AS currency_avg_val,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY val DESC NULLS LAST) AS val_dense_rank
    FROM base
    WINDOW w_grp AS (PARTITION BY currency ORDER BY val NULLS FIRST)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        currency,
        clean_currency,
        val_cume_dist_2,
        currency_avg_val,
        val_dense_rank,
        CUME_DIST() OVER (ORDER BY currency_avg_val) AS currency_avg_val_cume_dist,
        DENSE_RANK() OVER w_grp AS val_dense_rank_2,
        MAX(currency_avg_val) OVER (PARTITION BY currency) AS currency_max_currency_avg_val
    FROM enriched
    WINDOW w_grp AS (PARTITION BY currency ORDER BY val DESC NULLS LAST)
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        currency,
        clean_currency,
        currency_avg_val,
        val_dense_rank,
        val_dense_rank_2,
        currency_max_currency_avg_val,
        ROUND(currency_avg_val_cume_dist / val_dense_rank_2, 2) AS currency_avg_val_cume_dist_per_val_dense_rank_2,
        CASE WHEN val_dense_rank_2 >= 9 THEN 'High' WHEN val_dense_rank_2 >= 4 THEN 'Medium' ELSE 'Low' END AS val_dense_rank_2_band
    FROM with_windows
    WHERE val_dense_rank >= 5
)
SELECT
    currency_avg_val,
    val_dense_rank,
    val_dense_rank_2,
    currency_max_currency_avg_val,
    val_dense_rank_2_band
FROM cleaned
ORDER BY val_dense_rank_2, val_dense_rank DESC
