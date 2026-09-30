WITH joined_ranges AS (
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
per_group AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', event_at) AS event_at_month,
        ticket_id,
        MAX(event_at) AS last_event_at,
        ROUND(AVG(hours_since_open), 2) AS avg_hours_since_open,
        MIN(hours_since_open) AS min_hours_since_open
    FROM joined_ranges
    WHERE priority NOT IN ('P2', 'P1')
    GROUP BY DATE_TRUNC('MONTH', event_at), ticket_id
    HAVING MAX(hours_since_open) > 25.2
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.event_at_month,
        s.ticket_id,
        s.last_event_at,
        s.avg_hours_since_open,
        s.min_hours_since_open,
        t.channel AS ticket_channel,
        su.tier AS agent_tier,
        su.team AS agent_team
    FROM per_group s
    LEFT JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
with_parent_ticket AS (
    -- Join tickets
    SELECT s.*, t.requester_id AS ticket_requester_id
    FROM with_parent s
    LEFT JOIN tickets t ON t.id = s.ticket_id
)
-- Final projection
SELECT
    event_at_month,
    last_event_at,
    min_hours_since_open,
    agent_tier,
    ticket_requester_id,
    DATE_FORMAT(event_at_month, 'yyyy-MM-dd HH:00') AS event_at_month_label
FROM with_parent_ticket
ORDER BY event_at_month_label DESC, agent_tier
