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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 7 DAYS)
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_priority_by_hours_since_open, 3) AS hours_since_open_percent_rank,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY priority ORDER BY hours_since_open DESC, id))) / 60, 1) AS minutes_since_prev_event_at
    FROM joined_ranges
    WINDOW w_priority_by_hours_since_open AS (PARTITION BY priority ORDER BY hours_since_open DESC)
),
with_details AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.priority,
        s.agent_id,
        s.hours_since_open,
        s.hours_since_open_percent_rank,
        s.minutes_since_prev_event_at,
        su.team AS agent_team
    FROM sequenced s
    LEFT JOIN support_agents su ON su.id = s.agent_id
)
SELECT
    id,
    ticket_id,
    priority,
    agent_id,
    hours_since_open,
    minutes_since_prev_event_at,
    agent_team,
    ABS(hours_since_open - 28.8) AS hours_since_open_distance
FROM with_details
ORDER BY id DESC, priority DESC
LIMIT 20
