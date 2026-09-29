WITH in_window AS (
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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 3 DAYS)
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.agent_id,
        s.hours_since_open,
        su.agent_name,
        su.tier AS agent_tier,
        su.team AS agent_team,
        t.priority AS ticket_priority,
        t.opened_at AS ticket_opened_at
    FROM in_window s
    JOIN support_agents su ON su.id = s.agent_id
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        FLOOR(hours_since_open / 10) * 10 AS hours_since_open_bucket,
        COALESCE(hours_since_open, 0.0) AS hours_since_open_filled
    FROM combined_details
)
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    agent_id,
    agent_name,
    agent_tier,
    agent_team,
    ticket_priority,
    ticket_opened_at,
    hours_since_open - hours_since_open_filled AS hours_since_open_minus_hours_since_open_filled,
    DATE_TRUNC('DAY', event_at) AS event_at_day
FROM labeled
ORDER BY event_type DESC
