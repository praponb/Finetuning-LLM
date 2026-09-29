WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(discounts) e AS tier_value
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN currency = 'USD' THEN 1 ELSE 0 END AS is_usd
    FROM exploded
    WHERE tier_value > 62
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY is_usd NULLS LAST) AS is_usd_cume_dist,
        MAX(is_usd) OVER (PARTITION BY currency) AS currency_max_is_usd,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY is_usd DESC NULLS LAST) AS is_usd_cume_dist_2,
        SUM(is_usd) OVER (PARTITION BY currency) AS currency_sum_is_usd,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY is_usd) AS is_usd_cume_dist_3
    FROM valid
),
enriched AS (
    -- Ranking and gap features
    SELECT
        is_usd,
        currency_max_is_usd,
        is_usd_cume_dist_2,
        SUM(is_usd_cume_dist_3) OVER (PARTITION BY currency) AS currency_sum_is_usd_cume_dist_3,
        STDDEV_SAMP(tier_value) OVER (PARTITION BY currency) AS currency_stddev_samp_tier_value,
        RANK() OVER (PARTITION BY currency ORDER BY is_usd_cume_dist_3 NULLS LAST) AS is_usd_cume_dist_3_rank,
        STDDEV_SAMP(tier_value) OVER (PARTITION BY currency) AS currency_stddev_samp_tier_value_2
    FROM scored
    WHERE currency_sum_is_usd >= 260
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(currency_stddev_samp_tier_value_2, 0) AS currency_stddev_samp_tier_value_2_rounded
    FROM enriched
    WHERE is_usd_cume_dist_2 >= 0.71
),
grouped AS (
    -- Grouped metrics
    SELECT
        CASE WHEN is_usd_cume_dist_3_rank >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS is_usd_cume_dist_3_rank_tier,
        CASE WHEN is_usd >= 81 THEN 'LARGE' ELSE 'SMALL' END AS is_usd_tier,
        ROUND(AVG(is_usd), 2) AS avg_is_usd,
        PERCENTILE_APPROX(currency_max_is_usd, ARRAY(0.25, 0.5, 0.75)) AS currency_max_is_usd_quartiles,
        PERCENTILE_APPROX(currency_stddev_samp_tier_value, 0.9) AS currency_stddev_samp_tier_value_p90,
        PERCENT_RANK() OVER (PARTITION BY CASE WHEN is_usd_cume_dist_3_rank >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END ORDER BY PERCENTILE_APPROX(currency_stddev_samp_tier_value, 0.9) DESC) AS currency_stddev_samp_tier_value_p90_percent_rank
    FROM base
    GROUP BY CASE WHEN is_usd_cume_dist_3_rank >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN is_usd >= 81 THEN 'LARGE' ELSE 'SMALL' END
    HAVING avg_is_usd >= 0
)
SELECT
    is_usd_cume_dist_3_rank_tier,
    is_usd_tier,
    currency_max_is_usd_quartiles,
    currency_stddev_samp_tier_value_p90
FROM grouped
ORDER BY is_usd_tier DESC NULLS FIRST
