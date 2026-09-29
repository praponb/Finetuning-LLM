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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 3 DAYS)
),
per_group AS (
    -- Summarise per group
    SELECT
        ticket_id,
        TO_DATE(event_at) AS event_at_day,
        MIN(hours_since_open) AS min_hours_since_open,
        MIN(event_at) AS first_event_at,
        SUM(CASE WHEN event_type = 'replied' THEN 1 ELSE 0 END) AS n_event_type_replied,
        SUM(hours_since_open) FILTER (WHERE priority IS NOT NULL) AS hours_since_open_priority_is_not_null,
        ROUND(SUM(hours_since_open), 2) AS total_hours_since_open
    FROM joined_ranges
    GROUP BY 1, 2
    HAVING MAX(hours_since_open) > 14.4
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN total_hours_since_open >= 324.0 THEN 'HIGH' WHEN total_hours_since_open >= 108.0 THEN 'MEDIUM' ELSE 'LOW' END AS total_hours_since_open_tier,
        first_event_at + INTERVAL 30 MINUTES AS first_event_at_plus_30m,
        ROUND(total_hours_since_open * 0.25, 2) AS total_hours_since_open_adj
    FROM per_group
    WHERE event_at_day < DATE '2025-05-01'
)
-- Final projection
SELECT
    ticket_id,
    event_at_day,
    min_hours_since_open,
    first_event_at,
    n_event_type_replied,
    hours_since_open_priority_is_not_null,
    total_hours_since_open,
    total_hours_since_open_tier,
    total_hours_since_open_adj,
    COALESCE(min_hours_since_open, 0.0) AS min_hours_since_open_filled
FROM scored
WHERE first_event_at < TIMESTAMP '2025-02-24 10:00:00'
ORDER BY hours_since_open_priority_is_not_null DESC, n_event_type_replied DESC
