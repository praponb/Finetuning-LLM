WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_prices) e AS val
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(val) OVER w_recent AS currency_max_val,
        AVG(val) OVER w_recent AS currency_avg_val,
        AVG(val) OVER w_recent AS currency_avg_val_2
    FROM flattened
    WHERE currency IN ('EUR', 'THB')
    WINDOW w_recent AS (PARTITION BY currency)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        RANK() OVER (PARTITION BY currency ORDER BY currency_avg_val DESC) AS currency_avg_val_rank,
        PERCENT_RANK() OVER w_ordered AS currency_avg_val_percent_rank,
        RANK() OVER w_grp AS val_rank,
        MIN(currency_avg_val_2) OVER (PARTITION BY currency) AS currency_min_currency_avg_val_2,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY val ASC) AS val_cume_dist
    FROM scored
    WINDOW w_ordered AS (PARTITION BY currency ORDER BY currency_avg_val DESC NULLS FIRST), w_grp AS (PARTITION BY currency ORDER BY val DESC NULLS FIRST)
),
metrics_price_tiers AS (
    -- Ranking and gap features
    SELECT
        effective_from,
        currency,
        val,
        currency_avg_val_2,
        currency_avg_val_rank,
        currency_avg_val_percent_rank,
        val_cume_dist,
        AVG(currency_max_val) OVER (PARTITION BY currency) AS currency_avg_currency_max_val,
        AVG(currency_avg_val) OVER (PARTITION BY currency) AS currency_avg_currency_avg_val,
        ROUND(currency_avg_val_rank / SUM(currency_avg_val_rank) OVER (PARTITION BY currency), 4) AS currency_avg_val_rank_share,
        DENSE_RANK() OVER (ORDER BY currency_avg_val_rank DESC NULLS LAST) AS currency_avg_val_rank_dense_rank
    FROM metrics
)
SELECT
    effective_from,
    currency,
    currency_avg_val_2,
    currency_avg_val_percent_rank,
    val_cume_dist,
    currency_avg_currency_max_val,
    currency_avg_currency_avg_val,
    currency_avg_val_rank_share,
    currency_avg_val_rank_dense_rank,
    CASE WHEN currency RLIKE '^(USD|EUR)$' THEN 'GROUP_B' WHEN currency RLIKE '^(THB)$' THEN 'EXTENDED' ELSE 'PRIORITY' END AS currency_zone
FROM metrics_price_tiers;
