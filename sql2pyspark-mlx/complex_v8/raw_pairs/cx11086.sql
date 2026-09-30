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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 1 DAYS)
),
screened AS (
    -- Rows with no matching tickets
    SELECT s.*
    FROM in_window s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND t.channel = 'email'
              AND t.csat < 4
        )
      AND s.event_type = 'assigned'
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT agent_id, COUNT(*) AS ticket_count
    FROM tickets
    GROUP BY agent_id
),
with_stats AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        COALESCE(t.ticket_count, 0) AS ticket_count
    FROM screened s
    LEFT JOIN ticket_stats t ON t.agent_id = s.agent_id
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        LAST_VALUE(ticket_count) IGNORE NULLS OVER (PARTITION BY agent_id ORDER BY hours_since_open NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_ticket_count,
        ROUND(hours_since_open / NULLIF(SUM(hours_since_open) OVER (PARTITION BY agent_id, priority), 0), 4) AS hours_since_open_share_of_agent_id_priority,
        FIRST_VALUE(event_at) OVER (PARTITION BY agent_id ORDER BY hours_since_open NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at
    FROM with_stats
)
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    priority,
    agent_id,
    hours_since_open,
    last_known_ticket_count,
    hours_since_open_share_of_agent_id_priority,
    GREATEST(ticket_count, last_known_ticket_count) AS greatest_ticket_count_last_known_ticket_count
FROM sequenced
ORDER BY greatest_ticket_count_last_known_ticket_count, id
LIMIT 20
