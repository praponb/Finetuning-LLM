WITH recent_tickets AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        opened_at,
        priority,
        channel,
        csat,
        LENGTH(TRIM(channel)) AS channel_len,
        ROUND(csat / NULLIF(requester_id, 0), 3) AS csat_per_requester_id,
        INITCAP(TRIM(priority)) AS priority_clean
    FROM tickets
    WHERE ARRAY_CONTAINS(tags, 'upgrade')
      AND HOUR(opened_at) BETWEEN 9 AND 14
),
qualified AS (
    -- Rows with no matching tickets
    SELECT s.*
    FROM recent_tickets s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND t.opened_at < TIMESTAMP '2025-02-18 07:00:00'
              AND t.csat < 4
        )
),
with_details_agent AS (
    -- Attach agent attributes
    SELECT s.*, su.team AS agent_team, su.hired_on AS agent_hired_on
    FROM qualified s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
exploded_detail_events AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.priority,
        s.channel,
        s.csat,
        s.channel_len,
        s.csat_per_requester_id,
        s.agent_team,
        s.agent_hired_on,
        t.id AS event_id,
        t.event_at,
        t.actor AS event_actor,
        t.event_type
    FROM with_details_agent s
    JOIN ticket_events t ON t.ticket_id = s.id AND t.event_at >= TIMESTAMP '2025-01-25 08:00:00'
)
SELECT
    csat,
    COALESCE(priority, 'ALL') AS priority,
    COUNT(DISTINCT agent_team) AS distinct_agent_teams,
    MIN(csat_per_requester_id) AS min_csat_per_requester_id,
    MAX(agent_hired_on) AS last_agent_hired_on
FROM exploded_detail_events
GROUP BY CUBE(csat, priority)
ORDER BY csat NULLS LAST, priority NULLS LAST
