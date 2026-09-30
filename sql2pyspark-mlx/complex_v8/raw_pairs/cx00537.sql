WITH filtered_agents AS (
    -- Scope the support_agents rows
    SELECT
        id,
        agent_name,
        team,
        tier,
        hired_on,
        LAST_DAY(hired_on) AS hired_on_month_end
    FROM support_agents
    WHERE tier = 'T2'
),
scored AS (
    -- Derive helper columns
    SELECT *, LENGTH(TRIM(agent_name)) AS agent_name_len
    FROM filtered_agents
    WHERE team IN ('Onboarding', 'VIP')
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM scored
    WHERE agent_name_len > (SELECT AVG(agent_name_len) FROM scored)
),
unique_rows AS (
    -- Keep the latest row per tier, team
    SELECT
        id,
        agent_name,
        team,
        tier,
        hired_on,
        hired_on_month_end,
        agent_name_len
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY tier, team ORDER BY agent_name_len DESC, id DESC) AS rn
        FROM qualified
    ) ranked
    WHERE rn = 1
)
-- Subtotals with GROUPING SETS
SELECT
    agent_name_len,
    team,
    COUNT(DISTINCT id) AS distinct_ids,
    PERCENTILE_APPROX(agent_name_len, 0.5) AS median_agent_name_len
FROM unique_rows
GROUP BY GROUPING SETS ((agent_name_len, team), (agent_name_len), ())
ORDER BY agent_name_len NULLS LAST, team NULLS LAST
