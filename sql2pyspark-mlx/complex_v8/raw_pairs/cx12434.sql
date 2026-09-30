WITH covered AS (
    -- Events that happened while the ticket was open
    SELECT
        e.id,
        e.ticket_id,
        e.event_at,
        e.event_type,
        t.priority,
        t.agent_id,
        ROUND((UNIX_TIMESTAMP(e.event_at) - UNIX_TIMESTAMP(t.opened_at)) / 3600, 2) AS hours_since_open
    FROM ticket_events e
    JOIN tickets t
      ON t.id = e.ticket_id
     AND e.event_at >= t.opened_at
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 2 DAYS)
),
unique_rows AS (
    -- Keep the latest row per ticket_id, agent_id
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        priority,
        agent_id,
        hours_since_open
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY ticket_id, agent_id ORDER BY event_at DESC, id DESC) AS row_num
        FROM covered
    ) ranked
    WHERE row_num = 1
),
enriched AS (
    -- Join support_agents
    SELECT
        s.*,
        su.hired_on AS agent_hired_on,
        su.team AS agent_team,
        su.agent_name,
        t.priority AS ticket_priority,
        t.requester_id AS ticket_requester_id
    FROM unique_rows s
    JOIN support_agents su ON su.id = s.agent_id
    LEFT JOIN tickets t ON t.id = s.ticket_id
)
SELECT
    COALESCE(agent_team, 'ALL') AS agent_team,
    COALESCE(ticket_priority, 'ALL') AS ticket_priority,
    COALESCE(event_type, 'ALL') AS event_type,
    ROUND(100 * COUNT(CASE WHEN event_type NOT IN ('created', 'reopened') THEN 1 END) / COUNT(*), 1) AS pct_event_type_not_in_created_re,
    COUNT(*) AS n_rows,
    COUNT(DISTINCT priority) AS distinct_prioritys
FROM enriched
GROUP BY CUBE(agent_team, ticket_priority, event_type)
ORDER BY agent_team NULLS LAST, ticket_priority NULLS LAST, event_type NULLS LAST
