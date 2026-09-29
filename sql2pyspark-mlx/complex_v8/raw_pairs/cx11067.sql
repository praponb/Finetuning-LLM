WITH base AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        csat,
        tags
    FROM tickets
    WHERE opened_at >= TIMESTAMP '2025-01-25 11:00:00'
),
combined_details_agent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.priority,
        s.csat,
        s.tags,
        su.hired_on AS agent_hired_on
    FROM base s
    JOIN support_agents su ON su.id = s.agent_id AND su.hired_on IS NOT NULL
    WHERE su.hired_on IS NOT NULL
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        tags,
        agent_hired_on,
        SIZE(tags) AS tags_count,
        DATE_TRUNC('WEEK', closed_at) AS closed_at_week
    FROM combined_details_agent
    WHERE MONTH(closed_at) IN (2, 3)
),
screened AS (
    -- Compare against the overall min
    SELECT *
    FROM scored
    WHERE tags_count > (SELECT MIN(tags_count) FROM scored)
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.tags,
        s.agent_hired_on,
        s.tags_count,
        s.closed_at_week,
        su.team AS agent_team,
        su.tier AS agent_tier,
        su.hired_on AS agent_hired_on_2
    FROM screened s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
agent_tier_stats AS (
    -- Avg tags_count per agent_tier
    SELECT agent_tier, AVG(tags_count) AS avg_tags_count
    FROM with_details
    GROUP BY agent_tier
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_tags_count,
        ROUND(s.tags_count - g.avg_tags_count, 2) AS tags_count_minus_avg
    FROM with_details s
    JOIN agent_tier_stats g ON g.agent_tier = s.agent_tier
    WHERE s.tags_count > g.avg_tags_count
)
-- Final projection
SELECT
    id,
    closed_at,
    tags,
    agent_hired_on,
    tags_count,
    closed_at_week,
    agent_tier,
    avg_tags_count,
    tags_count_minus_avg,
    closed_at + INTERVAL 1 DAYS AS closed_at_plus_1d,
    CASE WHEN tags_count >= 2 THEN 'A' WHEN tags_count >= 1 THEN 'B' ELSE 'C' END AS tags_count_tier
FROM vs_group
ORDER BY closed_at_plus_1d DESC NULLS FIRST
