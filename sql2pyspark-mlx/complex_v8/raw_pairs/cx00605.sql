WITH matched AS (
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
with_activity AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM matched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND TO_DATE(t.event_at) < DATE '2025-02-18'
              AND t.event_type IN ('resolved', 'created')
        )
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(hours_since_open) OVER (PARTITION BY event_type, priority ORDER BY event_at, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS hours_since_open_max_3,
        SUM(hours_since_open) OVER (PARTITION BY agent_id ORDER BY hours_since_open ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_hours_since_open,
        ROUND(hours_since_open - AVG(hours_since_open) OVER w_ticket_id_event_type, 2) AS hours_since_open_vs_ticket_id_event_type_avg,
        MAX(hours_since_open) OVER (PARTITION BY ticket_id, event_type) AS ticket_id_event_type_max_hours_since_open,
        NTILE(4) OVER w_agent_id_by_hours_since_open_2 AS hours_since_open_quartile
    FROM with_activity
    WINDOW w_ticket_id_event_type AS (PARTITION BY ticket_id, event_type), w_agent_id_by_hours_since_open_2 AS (PARTITION BY agent_id ORDER BY hours_since_open ASC, id)
),
with_activity_2 AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM metrics s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.event_type IN ('resolved', 'created')
        )
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    priority,
    hours_since_open,
    running_hours_since_open,
    hours_since_open_vs_ticket_id_event_type_avg,
    ticket_id_event_type_max_hours_since_open,
    hours_since_open_quartile,
    DATE_TRUNC('WEEK', event_at) AS event_at_week
FROM with_activity_2
WHERE event_type NOT IN ('resolved', 'replied')
ORDER BY event_at, ticket_id
