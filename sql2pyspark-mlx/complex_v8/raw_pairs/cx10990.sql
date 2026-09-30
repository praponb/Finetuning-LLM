WITH prepared_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        channel,
        csat,
        tags,
        DATE_FORMAT(opened_at, 'yyyy-MM-dd HH:00') AS opened_at_label
    FROM tickets
    WHERE closed_at >= TIMESTAMP '2025-01-14 23:00:00'
      AND csat > 2
),
flattened AS (
    -- One row per tag
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.priority,
        s.channel,
        s.csat,
        s.opened_at_label,
        t.tag
    FROM prepared_tickets s
    LATERAL VIEW OUTER EXPLODE(s.tags) t AS tag
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM flattened
    WHERE csat > (SELECT AVG(csat) FROM flattened)
),
tagged AS (
    -- Derive helper columns
    SELECT *, COALESCE(priority, 'UNKNOWN') AS priority_or_unknown
    FROM with_activity
),
line_level_events AS (
    -- One row per event
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.priority,
        s.opened_at_label,
        s.tag,
        s.priority_or_unknown,
        t.id AS event_id,
        t.event_type,
        t.actor AS event_actor
    FROM tagged s
    JOIN ticket_events t ON t.ticket_id = s.id AND t.event_type IN ('assigned', 'reopened')
),
summary AS (
    -- Summarise per group
    SELECT
        priority_or_unknown,
        event_type,
        COUNT(*) AS row_count,
        PERCENTILE_APPROX(requester_id, 0.5) AS median_requester_id,
        COUNT(*) FILTER (WHERE event_actor = 'system') AS n_event_actor_system
    FROM line_level_events
    GROUP BY priority_or_unknown, event_type
    HAVING SUM(requester_id) > 7
)
SELECT *
FROM (
    SELECT row_count, event_type, median_requester_id
    FROM summary
) src
PIVOT (
    SUM(median_requester_id) AS amount, COUNT(median_requester_id) AS n
    FOR event_type IN ('replied', 'escalated', 'resolved')
)
ORDER BY row_count
