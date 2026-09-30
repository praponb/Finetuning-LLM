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
rollup_base AS (
    -- Group-level totals
    SELECT
        priority,
        COUNT(*) AS n_rows,
        MAX(event_at) AS last_event_at,
        PERCENTILE_APPROX(hours_since_open, 0.5) AS median_hours_since_open,
        ARRAY_SORT(COLLECT_SET(event_type)) AS event_type_list,
        SUM(hours_since_open) AS total_hours_since_open,
        SUM(CASE WHEN event_type <> 'resolved' THEN hours_since_open ELSE 0 END) AS hours_since_open_event_type_resolved
    FROM covered
    GROUP BY 1
    HAVING SUM(hours_since_open) > 21.6
)
SELECT
    priority,
    n_rows,
    last_event_at,
    median_hours_since_open,
    total_hours_since_open,
    hours_since_open_event_type_resolved,
    median_hours_since_open - n_rows AS median_hours_since_open_minus_n_rows
FROM rollup_base
WHERE median_hours_since_open BETWEEN 18.0 AND 61.2
ORDER BY priority DESC NULLS FIRST, median_hours_since_open_minus_n_rows DESC
