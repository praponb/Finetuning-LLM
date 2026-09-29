WITH scoped AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        channel,
        csat,
        HOUR(closed_at) AS closed_at_hour
    FROM tickets
    WHERE agent_id IS NOT NULL
),
screened AS (
    -- Keep rows whose agent matches
    SELECT s.*
    FROM scoped s
    WHERE EXISTS (
            SELECT 1
            FROM support_agents su
            WHERE su.id = s.agent_id
              AND su.agent_name IS NOT NULL
        )
),
combined_details_agent AS (
    -- Attach agent attributes
    SELECT s.*, su.agent_name, su.team AS agent_team
    FROM screened s
    JOIN support_agents su ON su.id = s.agent_id
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        channel,
        FLOOR(closed_at_hour / 2) * 2 AS closed_at_hour_bucket,
        COALESCE(agent_team, 'UNKNOWN') AS agent_team_or_unknown
    FROM combined_details_agent
)
SELECT
    agent_id,
    closed_at_hour_bucket,
    agent_team_or_unknown,
    GROUPING_ID() AS grouping_level,
    MAX(closed_at) AS last_closed_at,
    ARRAY_SORT(COLLECT_SET(channel)) AS channel_list
FROM tagged
GROUP BY CUBE(agent_id, closed_at_hour_bucket, agent_team_or_unknown)
