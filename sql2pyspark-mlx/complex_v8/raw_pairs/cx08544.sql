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
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        TO_DATE(event_at) AS event_at_day,
        priority,
        SUM(hours_since_open) AS total_hours_since_open,
        MAX(hours_since_open) AS max_hours_since_open
    FROM joined_ranges
    GROUP BY 1, 2
    HAVING SUM(hours_since_open) > 10.8
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        ABS(total_hours_since_open - 129.6) AS total_hours_since_open_distance,
        ABS(max_hours_since_open - 50.4) AS max_hours_since_open_distance,
        GREATEST(max_hours_since_open, total_hours_since_open) AS greatest_max_hours_since_open_total_hours_since_
    FROM grouped
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN priority IN ('P1', 'P2') THEN 'group_a' WHEN priority = 'P3' THEN 'group_b' ELSE 'other' END AS priority_group
    FROM scored
    WHERE greatest_max_hours_since_open_total_hours_since_ > 43.2
)
SELECT
    event_at_day,
    max_hours_since_open,
    max_hours_since_open_distance,
    greatest_max_hours_since_open_total_hours_since_,
    priority_group,
    DATE_FORMAT(event_at_day, 'yyyy-MM-dd') AS event_at_day_label
FROM annotated
ORDER BY event_at_day DESC NULLS FIRST
