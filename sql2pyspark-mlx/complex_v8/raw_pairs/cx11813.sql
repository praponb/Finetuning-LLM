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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 2 DAYS)
),
grouped AS (
    -- Group-level totals
    SELECT
        DATE_FORMAT(event_at, 'yyyy-MM') AS event_at_ym,
        event_type,
        ticket_id,
        ROUND(AVG(hours_since_open), 2) AS avg_hours_since_open,
        MAX(hours_since_open) AS max_hours_since_open,
        SUM(CASE WHEN priority IN ('P1', 'P3', 'P4') THEN hours_since_open ELSE 0 END) AS hours_since_open_priority_in_p1_p3_p4,
        MAX(event_at) AS last_event_at,
        ARRAY_SORT(COLLECT_SET(priority)) AS priority_list,
        SUM(hours_since_open) AS total_hours_since_open
    FROM in_window
    GROUP BY DATE_FORMAT(event_at, 'yyyy-MM'), event_type, ticket_id
    HAVING SUM(hours_since_open) > 28.8
),
without_matches AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM grouped s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.actor IS NOT NULL
        )
      AND s.hours_since_open_priority_in_p1_p3_p4 >= 126.0
)
SELECT
    event_at_ym,
    event_type,
    avg_hours_since_open,
    max_hours_since_open,
    hours_since_open_priority_in_p1_p3_p4,
    priority_list,
    total_hours_since_open,
    ABS(total_hours_since_open - 280.8) AS total_hours_since_open_distance,
    CASE WHEN avg_hours_since_open >= 46.8 THEN 'HIGH' WHEN avg_hours_since_open >= 25.2 THEN 'MEDIUM' ELSE 'LOW' END AS avg_hours_since_open_tier
FROM without_matches
WHERE avg_hours_since_open >= 21.6
ORDER BY avg_hours_since_open
