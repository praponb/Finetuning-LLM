WITH flattened AS (
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE(tier_prices) AS tier_value
    FROM price_tiers
),
enriched AS (
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY tier_value DESC) AS tier_value_dense_rank,
        COUNT(tier_value) OVER (PARTITION BY plan, currency) AS plan_count_tier_value,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY tier_value DESC) AS tier_value_cume_dist,
        STDDEV_SAMP(tier_value) OVER (PARTITION BY currency) AS currency_stddev_samp_tier_value
    FROM flattened
),
with_windows AS (
    SELECT
        *,
        DENSE_RANK() OVER w_running AS tier_value_dense_rank_2,
        AVG(tier_value_dense_rank) OVER w_part AS plan_avg_tier_value_dense_rank
    FROM enriched
    WINDOW w_running AS (ORDER BY tier_value DESC NULLS FIRST), w_part AS (PARTITION BY plan)
),
with_windows_price_tiers AS (
    SELECT
        id,
        plan,
        effective_from,
        currency,
        tier_value,
        tier_value_dense_rank,
        plan_count_tier_value,
        tier_value_cume_dist,
        currency_stddev_samp_tier_value,
        tier_value_dense_rank_2,
        plan_avg_tier_value_dense_rank,
        AVG(tier_value) OVER (PARTITION BY plan, currency) AS plan_avg_tier_value,
        DENSE_RANK() OVER w_ordered AS tier_value_dense_rank_3,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY tier_value_dense_rank_2 DESC NULLS FIRST) AS tier_value_dense_rank_2_dense_rank
    FROM with_windows
    WINDOW w_ordered AS (PARTITION BY currency ORDER BY tier_value NULLS LAST)
)
SELECT
    currency,
    CASE WHEN tier_value_cume_dist >= 0.71 THEN 'HIGH' ELSE 'LOW' END AS tier_value_cume_dist_tier,
    DATE_TRUNC('YEAR', effective_from) AS year_start,
    SUM(tier_value) FILTER (WHERE currency = 'EUR') AS eur_total_tier_value,
    SUM(CASE WHEN currency = 'THB' THEN plan_count_tier_value ELSE 0 END) AS thb_plan_count_tier_value
FROM with_windows_price_tiers
GROUP BY currency, CASE WHEN tier_value_cume_dist >= 0.71 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('YEAR', effective_from)
