WITH recent_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        channel,
        tags,
        LENGTH(TRIM(priority)) AS priority_len,
        CONCAT_WS(': ', priority, channel) AS priority_channel_label,
        TO_DATE(opened_at) AS opened_at_date
    FROM tickets
    WHERE HOUR(closed_at) BETWEEN 6 AND 11
),
filtered AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM recent_tickets s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.id
              AND t.event_at >= TIMESTAMP '2025-01-25 13:00:00'
        )
),
per_label AS (
    -- One row per tag
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.channel,
        s.priority_len,
        s.priority_channel_label,
        s.opened_at_date,
        t.tag_pos,
        t.tag
    FROM filtered s
    LATERAL VIEW POSEXPLODE(s.tags) t AS tag_pos, tag
),
with_children_events AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.channel,
        s.priority_len,
        s.priority_channel_label,
        s.opened_at_date,
        s.tag_pos,
        s.tag,
        t.id AS event_id,
        t.event_at,
        t.event_type
    FROM per_label s
    LEFT JOIN ticket_events t ON t.ticket_id = s.id
)
SELECT
    COALESCE(tag, 'ALL') AS tag,
    priority_len,
    tag_pos,
    MIN(event_at) AS first_event_at,
    COUNT(DISTINCT tag) AS distinct_tags
FROM with_children_events
GROUP BY GROUPING SETS ((tag, priority_len, tag_pos), (tag))
ORDER BY tag NULLS LAST, priority_len NULLS LAST, tag_pos NULLS LAST
