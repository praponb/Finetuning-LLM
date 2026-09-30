WITH flattened AS (
    SELECT
        id,
        plan,
        effective_from,
        element
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_prices) e AS element
),
valid AS (
    SELECT *, ROUND(element, 1) AS element_rounded
    FROM flattened
    WHERE effective_from > TO_DATE('2024-05-21')
      AND element BETWEEN 21 AND 80
),
with_windows AS (
    SELECT
        id,
        plan,
        effective_from,
        element_rounded,
        STDDEV_SAMP(element_rounded) OVER (PARTITION BY plan) AS plan_stddev_samp_element_rounded,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY element_rounded DESC NULLS FIRST) AS element_rounded_cume_dist,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY element DESC NULLS LAST) AS element_cume_dist,
        PERCENT_RANK() OVER (PARTITION BY MONTH(effective_from), plan ORDER BY element_rounded DESC) AS element_rounded_percent_rank,
        PERCENT_RANK() OVER w_hist AS element_rounded_percent_rank_2
    FROM valid
    WINDOW w_hist AS (PARTITION BY plan ORDER BY element_rounded NULLS LAST)
),
metrics AS (
    SELECT
        *,
        STDDEV_SAMP(element_cume_dist) OVER (PARTITION BY DATE_TRUNC('week', effective_from), plan) AS effective_from_period_stddev_samp_element_cume_dist,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY element_rounded_percent_rank_2 NULLS LAST) AS element_rounded_percent_rank_2_cume_dist,
        STDDEV_SAMP(plan_stddev_samp_element_rounded) OVER (PARTITION BY plan) AS plan_stddev_samp_plan_stddev_samp_element_rounded
    FROM with_windows
),
base AS (
    SELECT
        *,
        element_cume_dist / NULLIF(element_rounded_cume_dist, 0) AS element_cume_dist_per_element_rounded_cume_dist,
        CAST(element_rounded_percent_rank_2 AS INT) AS element_rounded_percent_rank_2_int
    FROM metrics
    WHERE effective_from > DATE '2024-02-11'
),
summary AS (
    SELECT
        plan,
        ROUND(AVG(element_rounded_cume_dist), 2) AS avg_element_rounded_cume_dist,
        MAX(plan_stddev_samp_plan_stddev_samp_element_rounded) AS max_plan_stddev_samp_plan_stddev_samp_element_rounded,
        COUNT_IF(plan = 'starter') AS starter_events,
        COUNT(DISTINCT plan) AS distinct_plans
    FROM base
    GROUP BY plan
)
SELECT
    plan,
    avg_element_rounded_cume_dist,
    max_plan_stddev_samp_plan_stddev_samp_element_rounded,
    starter_events
FROM summary
