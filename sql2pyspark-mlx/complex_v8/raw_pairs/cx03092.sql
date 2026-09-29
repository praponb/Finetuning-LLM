WITH valid AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        csat,
        tags,
        LENGTH(TRIM(priority)) AS priority_len,
        COALESCE(csat, 0) AS csat_filled,
        CONCAT_WS(' / ', channel, priority) AS channel_priority_label
    FROM tickets
    WHERE closed_at >= TIMESTAMP '2025-01-13 14:00:00'
),
enriched_agent AS (
    -- Enrich with dimension data
    SELECT s.*, su.tier AS agent_tier
    FROM valid s
    JOIN support_agents su ON su.id = s.agent_id AND YEAR(su.hired_on) = 2024
    WHERE su.tier <> 'T3'
),
detail_rows_events AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.priority,
        s.csat,
        s.priority_len,
        t.id AS event_id,
        t.event_at,
        t.actor AS event_actor
    FROM enriched_agent s
    JOIN ticket_events t ON t.ticket_id = s.id AND t.actor IS NOT NULL
),
joined_event AS (
    -- Join ticket_events
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.priority,
        s.csat,
        s.event_id,
        s.event_at,
        t.event_at AS event_event_at
    FROM detail_rows_events s
    LEFT JOIN ticket_events t ON t.id = s.event_id
),
with_parent_event AS (
    -- Join ticket_events
    SELECT s.*, t.event_type
    FROM joined_event s
    JOIN ticket_events t ON t.id = s.event_id
    WHERE t.event_type = 'reopened'
)
-- Final projection
SELECT
    id,
    agent_id,
    opened_at,
    priority,
    csat,
    event_at,
    event_event_at,
    event_type,
    COALESCE(csat, 0) AS csat_filled
FROM with_parent_event
WHERE event_id IS NOT NULL
ORDER BY id, event_at DESC
