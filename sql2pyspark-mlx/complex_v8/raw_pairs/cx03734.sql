WITH recent_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_type NOT IN ('replied', 'assigned')
      AND actor = 'customer'
),
screened AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM recent_events s
    WHERE s.ticket_id IN (
            SELECT t.ticket_id
            FROM ticket_events t
            WHERE t.actor = 'agent'
              AND MONTH(t.event_at) IN (1, 3)
        )
      AND s.actor NOT IN ('customer', 'system')
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        MAX(event_at) AS last_event_at,
        COUNT(*) AS event_count,
        MIN(event_at) AS first_event_at
    FROM ticket_events
    GROUP BY ticket_id
),
profiled AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.last_event_at,
        COALESCE(t.event_count, 0) AS event_count,
        t.first_event_at
    FROM screened s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM profiled
    WHERE event_count > (SELECT AVG(event_count) FROM profiled)
)
SELECT
    ticket_id,
    event_count,
    ROUND(AVG(event_count), 2) AS avg_event_count
FROM with_activity
GROUP BY GROUPING SETS ((ticket_id, event_count), (ticket_id), ())
ORDER BY ticket_id NULLS LAST, event_count NULLS LAST
