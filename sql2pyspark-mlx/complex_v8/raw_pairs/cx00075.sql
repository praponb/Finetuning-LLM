WITH matched AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.agent_id,
        a.opened_at,
        COUNT(b.id) AS prior_rows_12h,
        SUM(b.csat) AS prior_csat_12h
    FROM tickets a
    LEFT JOIN tickets b
      ON b.agent_id = a.agent_id
     AND b.opened_at < a.opened_at
     AND b.opened_at >= a.opened_at - INTERVAL 12 HOURS
    GROUP BY a.id, a.agent_id, a.opened_at
),
joined_agent AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.prior_rows_12h,
        su.team AS agent_team
    FROM matched s
    JOIN support_agents su ON su.id = s.agent_id AND su.tier IN ('T2', 'T1')
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        agent_team,
        CONCAT(agent_team, '#', CAST(agent_id AS STRING)) AS agent_team_ref
    FROM joined_agent
),
qualified AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM annotated s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.id
              AND t.actor = 'system'
              AND t.event_at >= TIMESTAMP '2025-01-13 11:00:00'
        )
)
-- Final projection
SELECT
    id,
    agent_team,
    agent_team_ref,
    SUBSTRING(TRIM(agent_team_ref), 1, 4) AS agent_team_ref_prefix,
    COALESCE(agent_team, 'UNKNOWN') AS agent_team_or_unknown
FROM qualified
WHERE agent_team_ref IS NOT NULL
ORDER BY agent_team
