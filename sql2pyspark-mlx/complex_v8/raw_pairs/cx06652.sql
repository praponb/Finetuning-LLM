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
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.priority,
        s.agent_id,
        s.hours_since_open,
        su.agent_name,
        t.csat AS ticket_csat,
        t.closed_at AS ticket_closed_at
    FROM joined_ranges s
    JOIN support_agents su ON su.id = s.agent_id
    LEFT JOIN tickets t ON t.id = s.ticket_id
    WHERE LENGTH(TRIM(su.agent_name)) >= 5
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT ticket_id, COUNT(*) AS event_count
    FROM ticket_events
    GROUP BY ticket_id
),
profiled AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        COALESCE(t.event_count, 0) AS event_count
    FROM with_details s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
)
-- Subtotals with GROUPING SETS
SELECT
    event_count,
    event_type,
    GROUPING(event_count) AS is_total_event_count,
    COUNT(*) AS n_rows
FROM profiled
GROUP BY GROUPING SETS ((event_count, event_type), (event_count), ())
ORDER BY event_count NULLS LAST, event_type NULLS LAST
