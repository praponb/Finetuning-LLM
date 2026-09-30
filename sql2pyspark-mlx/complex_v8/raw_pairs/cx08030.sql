WITH base AS (
    -- Filter support_agents
    SELECT
        id,
        agent_name,
        tier,
        hired_on,
        DAYOFWEEK(hired_on) AS hired_on_dayofweek
    FROM support_agents
    WHERE YEAR(hired_on) = 2024
      AND tier IN ('T1', 'T2')
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN tier IN ('T2', 'T1') THEN 'group_a' WHEN tier = 'T3' THEN 'group_b' ELSE 'other' END AS tier_group,
        UPPER(TRIM(tier)) AS tier_clean,
        DATE_FORMAT(hired_on, 'yyyy-MM-dd') AS hired_on_label
    FROM base
    WHERE LENGTH(TRIM(agent_name)) >= 8
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER w_all_by_hired_on AS hired_on_rank,
        ROUND(PERCENT_RANK() OVER (ORDER BY hired_on NULLS LAST), 3) AS hired_on_percent_rank,
        hired_on_dayofweek - LAG(hired_on_dayofweek) OVER (ORDER BY hired_on NULLS LAST, id) AS hired_on_dayofweek_change,
        MAX(hired_on_dayofweek) OVER (ORDER BY hired_on NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS hired_on_dayofweek_max_2,
        ROW_NUMBER() OVER (ORDER BY hired_on NULLS LAST, id) AS rn,
        SIZE(COLLECT_SET(tier_group) OVER (PARTITION BY hired_on_dayofweek)) AS distinct_tier_group_in_hired_on_dayofweek
    FROM enriched
    WINDOW w_all_by_hired_on AS (ORDER BY hired_on NULLS LAST, id)
)
SELECT *
FROM (
    SELECT rn, hired_on_rank, tier_group, hired_on_percent_rank
    FROM with_trends
) src
PIVOT (
    MAX(hired_on_percent_rank) AS max_v, SUM(hired_on_percent_rank) AS sum_v
    FOR tier_group IN ('group_b', 'group_a')
)
