WITH filtered AS (
    -- Filter support_agents
    SELECT
        id,
        agent_name,
        tier,
        hired_on,
        DAYOFWEEK(hired_on) AS hired_on_dayofweek,
        LENGTH(TRIM(tier)) AS tier_len
    FROM support_agents
    WHERE LENGTH(TRIM(agent_name)) >= 6
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(hired_on_dayofweek) OVER (PARTITION BY hired_on_dayofweek, tier), 2) AS hired_on_dayofweek_tier_avg_hired_on_dayofweek,
        SUM(tier_len) OVER (PARTITION BY hired_on_dayofweek, tier) AS hired_on_dayofweek_tier_sum_tier_len,
        LAG(hired_on_dayofweek, 2, 0) OVER w_tier_len_by_tier_len AS prev2_hired_on_dayofweek,
        LAST_VALUE(tier_len) IGNORE NULLS OVER (PARTITION BY tier_len ORDER BY tier_len ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_tier_len
    FROM filtered
    WINDOW w_tier_len_by_tier_len AS (PARTITION BY tier_len ORDER BY tier_len ASC NULLS LAST, id)
),
tagged AS (
    -- Add calculated columns
    SELECT *, LENGTH(TRIM(tier)) AS tier_len_2
    FROM ranked
    WHERE tier_len BETWEEN 4 AND 12
)
SELECT
    prev2_hired_on_dayofweek,
    tier_len_2,
    MAX(hired_on) AS last_hired_on,
    COUNT(*) AS row_count,
    COUNT(DISTINCT id) AS distinct_ids
FROM tagged
GROUP BY CUBE(prev2_hired_on_dayofweek, tier_len_2)
ORDER BY prev2_hired_on_dayofweek NULLS LAST, tier_len_2 NULLS LAST
