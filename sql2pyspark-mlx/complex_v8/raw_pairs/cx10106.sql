WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        channel,
        csat,
        tags,
        DATE_FORMAT(closed_at, 'yyyy-MM') AS closed_at_label
    FROM tickets
    WHERE channel = 'email'
      AND ARRAY_CONTAINS(tags, 'bug')
),
priority_stats AS (
    -- Avg requester_id per priority
    SELECT priority, AVG(requester_id) AS avg_requester_id
    FROM recent
    GROUP BY priority
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_requester_id,
        ROUND(s.requester_id - g.avg_requester_id, 2) AS requester_id_minus_avg
    FROM recent s
    JOIN priority_stats g ON g.priority = s.priority
    WHERE s.requester_id > g.avg_requester_id
),
without_matches AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM outliers s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND t.priority = 'P1'
              AND t.channel = 'web'
        )
      AND s.opened_at >= TIMESTAMP '2025-02-06 10:00:00'
),
with_children_events AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.tags,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_at
    FROM without_matches s
    JOIN ticket_events t ON t.ticket_id = s.id AND t.event_at < TIMESTAMP '2025-02-24 10:00:00'
),
flattened AS (
    -- One row per tag
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.event_id,
        s.event_actor,
        s.event_at,
        t.tag_pos,
        t.tag
    FROM with_children_events s
    LATERAL VIEW POSEXPLODE(s.tags) t AS tag_pos, tag
)
SELECT
    id,
    opened_at,
    event_id,
    event_actor,
    event_at,
    tag_pos,
    tag,
    DATEDIFF(DATE '2025-06-30', opened_at) AS days_since_opened_at,
    LOWER(REGEXP_REPLACE(TRIM(tag), ' ', '_')) AS tag_slug
FROM flattened
ORDER BY opened_at DESC NULLS FIRST
