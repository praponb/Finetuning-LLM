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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 7 DAYS)
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.priority,
        s.agent_id,
        s.hours_since_open,
        su.hired_on AS agent_hired_on,
        su.team AS agent_team,
        su.tier AS agent_tier
    FROM covered s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE su.hired_on < DATE '2025-02-27'
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT
        agent_id,
        MIN(closed_at) AS first_ticket_closed_at,
        COUNT(*) AS ticket_count,
        ROUND(AVG(csat), 2) AS avg_ticket_csat
    FROM tickets
    GROUP BY agent_id
),
summarized_rows AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        t.first_ticket_closed_at,
        COALESCE(t.ticket_count, 0) AS ticket_count,
        t.avg_ticket_csat
    FROM enriched s
    LEFT JOIN ticket_stats t ON t.agent_id = s.agent_id
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    priority,
    agent_id,
    hours_since_open,
    agent_hired_on,
    agent_team,
    agent_tier,
    first_ticket_closed_at,
    avg_ticket_csat
FROM summarized_rows
WHERE priority IS NOT NULL
ORDER BY agent_hired_on NULLS LAST, event_at
