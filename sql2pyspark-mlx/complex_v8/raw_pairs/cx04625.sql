WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        LOWER(REGEXP_REPLACE(TRIM(priority), ' ', '_')) AS priority_slug,
        SIZE(tags) AS tags_count
    FROM tickets
    WHERE priority IS NOT NULL
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM base
    WHERE requester_id > (SELECT AVG(requester_id) FROM base)
),
detail_rows AS (
    -- One row per event
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.closed_at,
        s.priority,
        s.channel,
        s.csat,
        s.priority_slug,
        s.tags_count,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_at,
        t.event_type
    FROM qualified s
    LEFT JOIN ticket_events t ON t.ticket_id = s.id
)
SELECT id, event_id, field, value
FROM (
    SELECT id, event_id, requester_id, csat
    FROM detail_rows
) src
UNPIVOT (
    value FOR field IN (requester_id, csat)
) unp
ORDER BY id, event_id, field
