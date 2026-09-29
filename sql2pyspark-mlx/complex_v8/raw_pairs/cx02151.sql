WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        csat,
        CONCAT_WS('|', channel, priority) AS channel_priority_label
    FROM tickets
    WHERE priority = 'P4'
),
unique_rows AS (
    -- Keep the latest row per agent_id, priority
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        csat,
        channel_priority_label
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY agent_id, priority ORDER BY closed_at DESC, id DESC) AS rn
        FROM valid
    ) ranked
    WHERE rn = 1
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.id) AS last_event_at,
        (SELECT COUNT(CASE WHEN t.actor = 'agent' THEN 1 END) FROM ticket_events t WHERE t.ticket_id = s.id) AS event_actor_count,
        (SELECT MIN(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.id) AS first_event_at
    FROM unique_rows s
)
SELECT
    id,
    requester_id,
    agent_id,
    closed_at,
    channel_priority_label,
    last_event_at,
    event_actor_count,
    LOWER(REGEXP_REPLACE(TRIM(priority), ' ', '_')) AS priority_slug,
    COALESCE(priority, 'UNKNOWN') AS priority_or_unknown
FROM summarized_rows
ORDER BY requester_id NULLS LAST
