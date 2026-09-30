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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 1 DAYS)
),
with_details AS (
    -- Join support_agents
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.priority,
        s.agent_id,
        s.hours_since_open,
        su.team AS agent_team,
        su.tier AS agent_tier,
        su.hired_on AS agent_hired_on
    FROM joined_ranges s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE su.team IN ('Billing', 'VIP')
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        FLOOR(hours_since_open / 10) * 10 AS hours_since_open_bucket,
        DATE_FORMAT(event_at, 'yyyy-MM') AS event_at_label,
        LAST_DAY(agent_hired_on) AS agent_hired_on_month_end
    FROM with_details
    WHERE ticket_id IS NOT NULL
)
-- Final projection
SELECT
    id,
    event_at,
    event_type,
    priority,
    agent_team,
    agent_tier,
    agent_hired_on,
    hours_since_open_bucket,
    event_at_label,
    TRUNC(agent_hired_on_month_end, 'MM') AS agent_hired_on_month_end_month_start
FROM scored
ORDER BY agent_team
