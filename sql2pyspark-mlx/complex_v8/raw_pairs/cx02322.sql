WITH matched AS (
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
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        ticket_id,
        event_type,
        priority,
        hours_since_open,
        ROW_NUMBER() OVER w_event_type_by_event_at AS rn,
        RANK() OVER w_event_type_by_event_at_ties AS event_at_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY event_type ORDER BY event_at DESC), 3) AS event_at_percent_rank,
        COUNT(*) OVER (PARTITION BY event_type ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count
    FROM matched
    WINDOW w_event_type_by_event_at AS (PARTITION BY event_type ORDER BY event_at DESC, id), w_event_type_by_event_at_ties AS (PARTITION BY event_type ORDER BY event_at DESC)
),
with_stats AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_count,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_count_2,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_count_3
    FROM ranked s
),
with_parent_ticket AS (
    -- Enrich with dimension data
    SELECT s.*, t.closed_at AS ticket_closed_at, t.priority AS ticket_priority
    FROM with_stats s
    LEFT JOIN tickets t ON t.id = s.ticket_id
)
SELECT
    event_count,
    rn,
    GROUPING(event_count) AS is_total_event_count,
    ROUND(100 * COUNT(CASE WHEN ticket_priority = 'P3' THEN 1 END) / COUNT(*), 1) AS pct_ticket_priority_p3,
    SUM(event_at_rank) AS total_event_at_rank,
    ARRAY_SORT(COLLECT_SET(priority)) AS priority_list
FROM with_parent_ticket
GROUP BY GROUPING SETS ((event_count, rn), (event_count), (rn), ())
ORDER BY event_count NULLS LAST, rn NULLS LAST
