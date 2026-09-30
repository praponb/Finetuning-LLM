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
profiled AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_count,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_count_2,
        (SELECT MIN(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS first_event_at
    FROM joined_ranges s
),
latest_per_key AS (
    -- Keep the latest row per event_type
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        priority,
        agent_id,
        hours_since_open,
        event_count,
        event_count_2,
        first_event_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_count DESC, id DESC) AS row_num
        FROM profiled
    ) ranked
    WHERE row_num = 1
),
stats AS (
    -- Group-level totals
    SELECT
        event_count,
        agent_id,
        SUM(event_count_2) AS total_event_count_2,
        ROUND(100 * COUNT(CASE WHEN priority <> 'P2' THEN 1 END) / COUNT(*), 1) AS pct_priority_p2,
        SUM(CASE WHEN event_type = 'resolved' THEN event_count ELSE 0 END) AS event_count_event_type_resolved
    FROM latest_per_key
    GROUP BY event_count, agent_id
    HAVING AVG(hours_since_open) > 18.0
)
-- Native SQL UNPIVOT (wide to long)
SELECT event_count, agent_id, metric_name, reading
FROM (
    SELECT event_count, agent_id, event_count_event_type_resolved, total_event_count_2
    FROM stats
) src
UNPIVOT INCLUDE NULLS (
    reading FOR metric_name IN (event_count_event_type_resolved, total_event_count_2)
) unp
ORDER BY event_count, agent_id, metric_name
