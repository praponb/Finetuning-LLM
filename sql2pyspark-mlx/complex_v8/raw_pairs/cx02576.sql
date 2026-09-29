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
    -- Group-level totals
    SELECT
        priority,
        agent_id,
        MAX(hours_since_open) AS max_hours_since_open,
        ARRAY_SORT(COLLECT_SET(event_type)) AS event_type_list,
        ROUND(100 * COUNT(CASE WHEN priority NOT IN ('P3', 'P4') THEN 1 END) / COUNT(*), 1) AS pct_priority_not_in_p3_p4,
        COUNT(*) AS row_count
    FROM joined_ranges
    WHERE agent_id >= 3
    GROUP BY priority, agent_id
    HAVING COUNT(*) >= 3
)
-- Native SQL UNPIVOT (wide to long)
SELECT priority, agent_id, metric, reading
FROM (
    SELECT priority, agent_id, max_hours_since_open, pct_priority_not_in_p3_p4
    FROM grouped
) src
UNPIVOT (
    reading FOR metric IN (max_hours_since_open, pct_priority_not_in_p3_p4)
) unp
