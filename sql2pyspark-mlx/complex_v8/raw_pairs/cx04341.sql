WITH valid_tickets AS (
    -- Scope the tickets rows
    SELECT *
    FROM tickets
    WHERE priority = 'P2'
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        opened_at + INTERVAL 30 MINUTES AS opened_at_plus_30m,
        TO_DATE(opened_at) AS opened_at_date,
        SUBSTRING(TRIM(channel), 1, 3) AS channel_prefix
    FROM valid_tickets
    WHERE csat BETWEEN 2 AND 4
),
with_parent_agent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.tags,
        s.opened_at_plus_30m,
        s.opened_at_date,
        s.channel_prefix,
        su.tier AS agent_tier
    FROM scored s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.agent_id,
        s.tags,
        s.agent_tier,
        su.team AS agent_team,
        su.hired_on AS agent_hired_on
    FROM with_parent_agent s
    JOIN support_agents su ON su.id = s.agent_id AND su.tier NOT IN ('T1', 'T3')
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        agent_id,
        tags,
        agent_tier,
        agent_hired_on,
        RANK() OVER w_agent_id_agent_team_by_agent AS agent_hired_on_rank,
        LAST_VALUE(agent_hired_on) IGNORE NULLS OVER (PARTITION BY agent_id, agent_team ORDER BY agent_hired_on ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_agent_hired_on,
        ROUND(PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', agent_hired_on), agent_team ORDER BY agent_hired_on), 3) AS agent_hired_on_percent_rank,
        RANK() OVER w_agent_id_agent_team_by_agent_3 AS agent_hired_on_rank_2,
        RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', agent_hired_on), agent_team ORDER BY agent_hired_on) AS agent_hired_on_rank_3
    FROM with_parent
    WINDOW w_agent_id_agent_team_by_agent AS (PARTITION BY agent_id, agent_team ORDER BY agent_hired_on ASC, id), w_agent_id_agent_team_by_agent_3 AS (PARTITION BY agent_id, agent_team ORDER BY agent_hired_on ASC)
),
filtered AS (
    -- Rows with no matching tickets
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND ARRAY_CONTAINS(t.tags, 'bug')
              AND t.csat >= 1
        )
),
joined_agent AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.tags,
        s.agent_hired_on,
        s.agent_hired_on_percent_rank,
        s.agent_hired_on_rank_2,
        s.agent_hired_on_rank_3,
        su.agent_name
    FROM filtered s
    JOIN support_agents su ON su.id = s.agent_id
)
SELECT
    id,
    agent_id,
    agent_hired_on_rank_2,
    agent_hired_on_rank_3,
    agent_name,
    DATEDIFF(DATE '2025-06-30', agent_hired_on) AS days_since_agent_hired_on,
    DATE_FORMAT(agent_hired_on, 'yyyy-MM-dd') AS agent_hired_on_label
FROM joined_agent
WHERE LOWER(agent_name) LIKE '%ir%'
ORDER BY agent_name DESC NULLS FIRST
