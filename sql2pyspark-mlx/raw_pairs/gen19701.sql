WITH exploded AS (
    SELECT
        id,
        plan,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_prices) li AS val
),
enriched AS (
    SELECT
        *,
        RANK() OVER (PARTITION BY plan ORDER BY val NULLS FIRST) AS val_rank,
        RANK() OVER (ORDER BY val DESC) AS val_rank_2
    FROM exploded
),
scored AS (
    SELECT
        *,
        SUM(val) OVER (PARTITION BY plan) AS plan_sum_val,
        CUME_DIST() OVER w_recent AS val_cume_dist,
        DENSE_RANK() OVER w_grp AS val_rank_dense_rank
    FROM enriched
    WHERE currency IN ('EUR', 'THB', 'USD')
    WINDOW w_recent AS (PARTITION BY plan ORDER BY val NULLS LAST), w_grp AS (PARTITION BY plan ORDER BY val_rank DESC)
),
scored_price_tiers AS (
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY val_rank_dense_rank NULLS FIRST) AS val_rank_dense_rank_cume_dist,
        MIN(val_rank_2) OVER (PARTITION BY plan, currency) AS plan_min_val_rank_2,
        PERCENT_RANK() OVER (ORDER BY val DESC NULLS FIRST) AS val_percent_rank,
        AVG(val) OVER (PARTITION BY currency) AS currency_avg_val
    FROM scored
)
SELECT
    CASE WHEN val_rank_2 >= 8 THEN 'LARGE' ELSE 'SMALL' END AS val_rank_2_tier,
    CASE WHEN plan_min_val_rank_2 >= 9 THEN 'LARGE' ELSE 'SMALL' END AS plan_min_val_rank_2_tier,
    SUM(val_rank_2) AS total_val_rank_2,
    COUNT(*) FILTER (WHERE plan = 'pro') AS pro_rows,
    COUNT(*) FILTER (WHERE plan = 'starter') AS starter_rows,
    COUNT_IF(currency = 'USD') AS usd_events
FROM scored_price_tiers
GROUP BY CASE WHEN val_rank_2 >= 8 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN plan_min_val_rank_2 >= 9 THEN 'LARGE' ELSE 'SMALL' END;
