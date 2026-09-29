WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE ticket_id <= 39
),
profiled AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_count,
        (SELECT MIN(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS first_event_at
    FROM scoped s
),
per_group AS (
    -- Summarise per group
    SELECT
        ticket_id,
        actor,
        MAX(first_event_at) AS last_first_event_at,
        ROUND(AVG(event_count), 2) AS avg_event_count,
        COUNT(DISTINCT ticket_id) AS distinct_ticket_ids,
        COUNT(DISTINCT event_type) AS distinct_event_types,
        MIN(first_event_at) AS first_first_event_at
    FROM profiled
    GROUP BY ticket_id, actor
),
with_parent_ticket AS (
    -- Attach ticket attributes
    SELECT s.*, t.channel AS ticket_channel, t.priority AS ticket_priority
    FROM per_group s
    JOIN tickets t ON t.id = s.ticket_id
)
-- Native SQL UNPIVOT (wide to long)
SELECT ticket_id, actor, metric, amount
FROM (
    SELECT ticket_id, actor, distinct_event_types, distinct_ticket_ids
    FROM with_parent_ticket
) src
UNPIVOT (
    amount FOR metric IN (distinct_event_types AS distinct_event_types_v, distinct_ticket_ids AS m_distinct_ticket_ids)
) unp
ORDER BY ticket_id, actor, metric
