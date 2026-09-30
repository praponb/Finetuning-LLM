WITH covered AS (
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
derived AS (
    -- Label and score rows
    SELECT
        id,
        event_at,
        event_type,
        priority,
        hours_since_open,
        COALESCE(hours_since_open, 0.0) AS hours_since_open_filled,
        DATE_TRUNC('YEAR', event_at) AS event_at_year
    FROM covered
    WHERE agent_id <= 6
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAST_VALUE(hours_since_open_filled) OVER (PARTITION BY priority, DATE_TRUNC('MONTH', event_at_year) ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_hours_since_open_filled,
        SUM(hours_since_open) OVER (PARTITION BY TO_DATE(event_at), priority) AS event_at_to_date_priority_sum_hours_since_open,
        ROUND(PERCENT_RANK() OVER (PARTITION BY priority, DATE_TRUNC('MONTH', event_at_year) ORDER BY event_at), 3) AS event_at_percent_rank,
        ROUND(hours_since_open_filled - AVG(hours_since_open_filled) OVER (PARTITION BY TO_DATE(event_at), priority), 2) AS hours_since_open_filled_vs_event_at_to_date_prio,
        ROUND(AVG(hours_since_open) OVER (PARTITION BY TO_DATE(event_at), priority), 2) AS event_at_to_date_priority_avg_hours_since_open
    FROM derived
)
SELECT
    id,
    event_at,
    event_type,
    priority,
    hours_since_open_filled,
    event_at_year,
    event_at_to_date_priority_sum_hours_since_open,
    event_at_percent_rank,
    hours_since_open_filled_vs_event_at_to_date_prio,
    event_at_to_date_priority_avg_hours_since_open
FROM ranked
ORDER BY event_at DESC
