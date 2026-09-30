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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 3 DAYS)
),
event_type_stats AS (
    -- Avg hours_since_open per event_type
    SELECT event_type, AVG(hours_since_open) AS avg_hours_since_open
    FROM in_window
    GROUP BY event_type
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_hours_since_open,
        ROUND(s.hours_since_open - g.avg_hours_since_open, 2) AS hours_since_open_minus_avg
    FROM in_window s
    JOIN event_type_stats g ON g.event_type = s.event_type
    WHERE s.hours_since_open > g.avg_hours_since_open
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        event_type,
        priority,
        avg_hours_since_open,
        COALESCE(priority, 'UNKNOWN') AS priority_or_unknown
    FROM outliers
    WHERE event_type = 'assigned'
),
per_group AS (
    -- Summarise per group
    SELECT
        event_type,
        priority,
        ARRAY_SORT(COLLECT_SET(event_type)) AS event_type_list,
        MAX(avg_hours_since_open) AS max_avg_hours_since_open
    FROM scored
    WHERE event_type = 'reopened'
    GROUP BY event_type, priority
)
SELECT *
FROM (
    SELECT priority, event_type, max_avg_hours_since_open
    FROM per_group
) src
PIVOT (
    MIN(max_avg_hours_since_open)
    FOR event_type IN ('created', 'replied', 'escalated')
)
ORDER BY priority
