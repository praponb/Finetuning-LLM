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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 3 DAYS)
),
rollup_base AS (
    -- Summarise per group
    SELECT
        YEAR(event_at) AS event_at_year,
        event_type,
        COUNT(*) AS row_count,
        COUNT(DISTINCT ticket_id) AS distinct_ticket_ids
    FROM covered
    GROUP BY YEAR(event_at), event_type
)
SELECT
    row_count,
    event_at_year,
    GROUPING_ID() AS grouping_level,
    COUNT(*) AS row_count_2,
    PERCENTILE_APPROX(row_count, 0.5) AS median_row_count,
    ARRAY_SORT(COLLECT_SET(event_type)) AS event_type_list
FROM rollup_base
GROUP BY CUBE(row_count, event_at_year)
ORDER BY row_count NULLS LAST, event_at_year NULLS LAST
