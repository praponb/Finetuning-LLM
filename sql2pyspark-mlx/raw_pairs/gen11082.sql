WITH exploded AS (
    SELECT
        id,
        plan,
        val
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_limits) e AS val
),
enriched AS (
    SELECT
        id,
        val,
        MAX(val) OVER w_ordered AS plan_max_val,
        STDDEV_SAMP(val) OVER w_ordered AS plan_stddev_samp_val,
        AVG(val) OVER w_ordered AS plan_avg_val
    FROM exploded
    WINDOW w_ordered AS (PARTITION BY plan)
),
enriched_price_tiers AS (
    SELECT
        val,
        plan_stddev_samp_val,
        plan_avg_val,
        PERCENT_RANK() OVER w_part AS val_percent_rank
    FROM enriched
    WHERE plan_max_val = 59
    WINDOW w_part AS (ORDER BY val DESC)
),
cleaned AS (
    SELECT *, val * plan_avg_val AS val_times_plan_avg_val
    FROM enriched_price_tiers
    WHERE val >= 75
)
SELECT
    CASE WHEN plan_avg_val >= 68 THEN 'LARGE' ELSE 'SMALL' END AS plan_avg_val_tier,
    CASE WHEN val_times_plan_avg_val >= 76 THEN 'HIGH' ELSE 'LOW' END AS val_times_plan_avg_val_tier,
    CASE WHEN val_percent_rank >= 0.83 THEN 'LARGE' ELSE 'SMALL' END AS val_percent_rank_tier,
    APPROX_PERCENTILE(plan_stddev_samp_val, ARRAY(0.25, 0.5, 0.75)) AS plan_stddev_samp_val_quartiles,
    APPROX_PERCENTILE(plan_stddev_samp_val, ARRAY(0.25, 0.5, 0.75)) AS plan_stddev_samp_val_quartiles_2,
    PERCENTILE_APPROX(plan_stddev_samp_val, 0.95) AS plan_stddev_samp_val_p95,
    GROUPING(CASE WHEN plan_avg_val >= 68 THEN 'LARGE' ELSE 'SMALL' END) AS gid
FROM cleaned
GROUP BY CUBE (CASE WHEN plan_avg_val >= 68 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN val_times_plan_avg_val >= 76 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN val_percent_rank >= 0.83 THEN 'LARGE' ELSE 'SMALL' END)
ORDER BY val_times_plan_avg_val_tier NULLS LAST
