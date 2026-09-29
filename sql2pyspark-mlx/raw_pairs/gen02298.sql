WITH lines AS (
    SELECT
        id,
        plan,
        effective_from,
        tier_value
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_limits) e AS tier_value
),
metrics AS (
    SELECT
        *,
        RANK() OVER (PARTITION BY plan ORDER BY tier_value DESC) AS tier_value_rank,
        DENSE_RANK() OVER (PARTITION BY MONTH(effective_from), plan ORDER BY tier_value NULLS FIRST) AS tier_value_dense_rank,
        AVG(tier_value) OVER (PARTITION BY plan) AS plan_avg_tier_value,
        PERCENT_RANK() OVER (PARTITION BY plan ORDER BY tier_value DESC) AS tier_value_percent_rank,
        PERCENT_RANK() OVER (PARTITION BY YEAR(effective_from), plan ORDER BY tier_value DESC NULLS FIRST) AS tier_value_percent_rank_2
    FROM lines
),
metrics_price_tiers AS (
    SELECT
        plan,
        tier_value,
        tier_value_rank,
        tier_value_dense_rank,
        plan_avg_tier_value,
        tier_value_percent_rank,
        RANK() OVER w_running AS plan_avg_tier_value_rank,
        PERCENT_RANK() OVER (PARTITION BY plan ORDER BY tier_value) AS tier_value_percent_rank_3
    FROM metrics
    WINDOW w_running AS (ORDER BY plan_avg_tier_value DESC NULLS FIRST)
),
cleaned AS (
    SELECT
        plan,
        tier_value_rank,
        plan_avg_tier_value,
        tier_value_percent_rank,
        plan_avg_tier_value_rank,
        tier_value_percent_rank_3,
        tier_value_rank - tier_value AS tier_value_rank_minus_tier_value
    FROM metrics_price_tiers
    WHERE tier_value_rank > 5
),
scored AS (
    SELECT
        *,
        STDDEV_SAMP(tier_value_percent_rank) OVER (PARTITION BY plan) AS plan_stddev_samp_tier_value_percent_rank,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY plan_avg_tier_value_rank DESC NULLS FIRST) AS plan_avg_tier_value_rank_cume_dist,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY tier_value_percent_rank_3 DESC) AS tier_value_percent_rank_3_cume_dist,
        RANK() OVER (PARTITION BY plan ORDER BY tier_value_rank) AS tier_value_rank_rank
    FROM cleaned
),
with_windows AS (
    SELECT
        plan_avg_tier_value,
        tier_value_percent_rank,
        plan_avg_tier_value_rank,
        tier_value_rank_minus_tier_value,
        plan_avg_tier_value_rank_cume_dist,
        tier_value_rank_rank,
        COUNT(tier_value_percent_rank) OVER (PARTITION BY plan) AS plan_count_tier_value_percent_rank,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY plan_avg_tier_value_rank_cume_dist) AS plan_avg_tier_value_rank_cume_dist_cume_dist
    FROM scored
)
SELECT
    plan_avg_tier_value_rank,
    tier_value_rank_rank,
    plan_avg_tier_value_rank_cume_dist_cume_dist,
    PERCENT_RANK() OVER w_grp AS tier_value_rank_rank_percent_rank,
    PERCENT_RANK() OVER (ORDER BY tier_value_percent_rank DESC NULLS LAST) AS tier_value_percent_rank_percent_rank,
    RANK() OVER (ORDER BY tier_value_percent_rank DESC NULLS FIRST) AS tier_value_percent_rank_rank,
    PERCENT_RANK() OVER w_hist AS plan_count_tier_value_percent_rank_percent_rank
FROM with_windows
WINDOW w_grp AS (ORDER BY tier_value_rank_rank DESC NULLS LAST), w_hist AS (ORDER BY plan_count_tier_value_percent_rank NULLS LAST);
