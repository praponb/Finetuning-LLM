WITH lines AS (
    SELECT
        id,
        plan,
        currency,
        EXPLODE_OUTER(tier_prices) AS val
    FROM price_tiers
),
metrics AS (
    SELECT
        id,
        plan,
        currency,
        val,
        PERCENT_RANK() OVER (PARTITION BY plan ORDER BY val DESC NULLS LAST) AS val_percent_rank,
        COUNT(val) OVER w_grp AS plan_count_val,
        COUNT(val) OVER w_grp AS plan_count_val_2,
        RANK() OVER (ORDER BY val DESC) AS val_rank
    FROM lines
    WINDOW w_grp AS (PARTITION BY plan)
),
enriched AS (
    SELECT
        *,
        SUM(val) OVER (PARTITION BY currency, plan) AS currency_sum_val,
        STDDEV_SAMP(plan_count_val_2) OVER (PARTITION BY currency) AS currency_stddev_samp_plan_count_val_2
    FROM metrics
),
metrics_price_tiers AS (
    SELECT
        id,
        plan,
        currency,
        val,
        val_percent_rank,
        plan_count_val,
        plan_count_val_2,
        val_rank,
        currency_sum_val,
        currency_stddev_samp_plan_count_val_2,
        PERCENT_RANK() OVER (ORDER BY val DESC NULLS FIRST) AS val_percent_rank_2,
        COUNT(currency_sum_val) OVER (PARTITION BY plan, currency) AS plan_count_currency_sum_val,
        ROUND(val_rank / SUM(val_rank) OVER (PARTITION BY currency), 4) AS val_rank_share,
        CUME_DIST() OVER (ORDER BY val NULLS LAST) AS val_cume_dist,
        ROUND(plan_count_val / SUM(plan_count_val) OVER (PARTITION BY plan), 4) AS plan_count_val_share
    FROM enriched
),
scored AS (
    SELECT
        *,
        RANK() OVER (ORDER BY val_rank DESC NULLS FIRST) AS val_rank_rank,
        PERCENT_RANK() OVER (ORDER BY val_percent_rank_2 NULLS LAST) AS val_percent_rank_2_percent_rank,
        ROUND(plan_count_val_2 / SUM(plan_count_val_2) OVER (PARTITION BY plan), 4) AS plan_count_val_2_share,
        RANK() OVER (PARTITION BY currency ORDER BY val_rank DESC NULLS LAST) AS val_rank_rank_2
    FROM metrics_price_tiers
)
SELECT
    currency,
    CASE WHEN plan_count_val_share >= 0.85 THEN 'LARGE' ELSE 'SMALL' END AS plan_count_val_share_tier,
    COUNT(*) FILTER (WHERE plan = 'basic') AS basic_rows,
    COUNT(*) AS event_count,
    SUM(val_percent_rank) AS total_val_percent_rank
FROM scored
GROUP BY currency, plan_count_val_share_tier
ORDER BY event_count DESC NULLS FIRST
