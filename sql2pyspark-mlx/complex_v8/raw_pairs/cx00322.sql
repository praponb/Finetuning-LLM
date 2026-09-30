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
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        ticket_id,
        event_at,
        priority,
        DENSE_RANK() OVER (PARTITION BY ticket_id, priority ORDER BY event_at) AS event_at_dense_rank,
        COUNT(*) OVER (PARTITION BY event_type, priority) AS rows_in_event_type_priority,
        ROUND(AVG(hours_since_open) OVER (PARTITION BY event_type, priority), 2) AS event_type_priority_avg_hours_since_open,
        SIZE(COLLECT_SET(ticket_id) OVER (PARTITION BY event_type, priority)) AS distinct_ticket_id_in_event_type_priority,
        ROUND(CUME_DIST() OVER (PARTITION BY ticket_id, priority ORDER BY event_at, id), 3) AS event_at_cume_dist
    FROM joined_ranges
),
exploded_detail AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.priority,
        s.rows_in_event_type_priority,
        s.event_type_priority_avg_hours_since_open,
        s.distinct_ticket_id_in_event_type_priority,
        s.event_at_cume_dist,
        t.id AS event_id,
        t.event_type,
        t.event_at AS event_event_at
    FROM with_trends s
    LEFT JOIN ticket_events t ON t.ticket_id = s.ticket_id AND TO_DATE(t.event_at) < DATE '2025-02-12'
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        ticket_id,
        priority,
        rows_in_event_type_priority,
        event_type_priority_avg_hours_since_open,
        event_id,
        event_type,
        event_event_at,
        COALESCE(event_at_cume_dist, 0.0) AS event_at_cume_dist_filled,
        COALESCE(event_at_cume_dist, 0.0) AS event_at_cume_dist_filled_2
    FROM exploded_detail
    WHERE priority = 'P3'
)
-- Final projection
SELECT
    id,
    ticket_id,
    priority,
    rows_in_event_type_priority,
    event_type_priority_avg_hours_since_open,
    event_type,
    event_at_cume_dist_filled,
    DATE_FORMAT(event_event_at, 'MMM yyyy') AS event_event_at_label,
    CONCAT(priority, '#', CAST(event_id AS STRING)) AS priority_ref
FROM scored
ORDER BY event_type
