WITH flattened AS (
    SELECT
        id,
        effective_from,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_limits) t AS tier_value
),
with_windows AS (
    SELECT
        *,
        RANK() OVER w_ordered AS tier_value_rank,
        MIN(tier_value) OVER (PARTITION BY currency) AS currency_min_tier_value,
        RANK() OVER (PARTITION BY currency ORDER BY tier_value NULLS FIRST) AS tier_value_rank_2,
        CUME_DIST() OVER (ORDER BY tier_value DESC NULLS LAST) AS tier_value_cume_dist
    FROM flattened
    WINDOW w_ordered AS (PARTITION BY currency ORDER BY tier_value)
),
cleaned AS (
    SELECT
        *,
        INITCAP(TRIM(currency)) AS currency_title,
        CASE WHEN currency = 'EUR' THEN 1 ELSE 0 END AS is_eur
    FROM with_windows
    WHERE effective_from > TO_DATE('2024-05-01')
      AND currency <> 'THB'
),
metrics AS (
    SELECT
        *,
        ROUND(tier_value_rank / SUM(tier_value_rank) OVER (PARTITION BY currency), 4) AS tier_value_rank_share,
        SUM(is_eur) OVER (PARTITION BY currency) AS currency_sum_is_eur
    FROM cleaned
),
base AS (
    SELECT
        *,
        CASE WHEN currency_min_tier_value >= 84 THEN 'High' WHEN currency_min_tier_value >= 39 THEN 'Medium' ELSE 'Low' END AS currency_min_tier_value_band,
        CEIL(tier_value_rank_2) AS tier_value_rank_2_whole
    FROM metrics
    WHERE effective_from >= DATE '2024-06-09'
)
SELECT
    CASE WHEN tier_value_rank_2 >= 7 THEN 'LARGE' ELSE 'SMALL' END AS tier_value_rank_2_tier,
    CASE WHEN tier_value >= 64 THEN 'HIGH' ELSE 'LOW' END AS tier_value_tier,
    CASE WHEN tier_value_rank_share >= 0.61 THEN 'LARGE' ELSE 'SMALL' END AS tier_value_rank_share_tier,
    ROUND(SUM(tier_value_rank_share) / NULLIF(COUNT(CASE WHEN currency = 'USD' THEN 1 END), 0), 2) AS tier_value_rank_share_per_usd,
    SUM(CASE WHEN currency = 'USD' THEN is_eur ELSE 0 END) AS usd_is_eur,
    COUNT(*) AS cnt,
    ROUND(SUM(tier_value_cume_dist) / NULLIF(COUNT(CASE WHEN currency = 'USD' THEN 1 END), 0), 2) AS tier_value_cume_dist_per_usd,
    SUM(tier_value_rank_2_whole) AS total_tier_value_rank_2_whole
FROM base
GROUP BY CASE WHEN tier_value_rank_2 >= 7 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN tier_value >= 64 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN tier_value_rank_share >= 0.61 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY total_tier_value_rank_2_whole DESC NULLS LAST;
