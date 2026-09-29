WITH cleaned AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        priority,
        channel,
        csat,
        tags
    FROM tickets
    WHERE closed_at >= TIMESTAMP '2025-01-13 14:00:00'
),
enriched_agent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.agent_id,
        s.priority,
        s.csat,
        s.tags,
        su.tier AS agent_tier,
        su.hired_on AS agent_hired_on
    FROM cleaned s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE su.tier NOT IN ('T2', 'T1')
),
with_activity AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM enriched_agent s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND t.closed_at < TIMESTAMP '2025-02-10 04:00:00'
        )
      AND s.csat > 2
),
with_children AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.priority,
        s.csat,
        s.tags,
        s.agent_hired_on,
        t.id AS event_id,
        t.event_type,
        t.event_at
    FROM with_activity s
    JOIN ticket_events t ON t.ticket_id = s.id
),
exploded_detail AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.priority,
        s.csat,
        s.agent_hired_on,
        s.event_id,
        t.id AS event_id_2,
        t.actor AS event_actor,
        t.event_type AS event_event_type,
        t.event_at AS event_event_at
    FROM with_children s
    JOIN ticket_events t ON t.ticket_id = s.id
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        DATE_TRUNC('YEAR', agent_hired_on) AS agent_hired_on_year,
        CASE WHEN event_event_type IN ('assigned', 'reopened') THEN 'group_a' WHEN event_event_type = 'resolved' THEN 'group_b' ELSE 'other' END AS event_event_type_group
    FROM exploded_detail
),
combined_details_event AS (
    -- Join ticket_events
    SELECT s.*, t.event_type
    FROM scored s
    JOIN ticket_events t ON t.id = s.event_id
)
SELECT
    id,
    agent_id,
    priority,
    csat,
    event_id,
    event_id_2,
    event_event_type,
    event_event_at,
    ROUND((UNIX_TIMESTAMP(event_event_at) - UNIX_TIMESTAMP(agent_hired_on_year)) / 3600, 1) AS hours_agent_hired_on_year_to_event_event_at,
    LOWER(REGEXP_REPLACE(TRIM(event_actor), ' ', '_')) AS event_actor_slug
FROM combined_details_event
ORDER BY event_actor_slug DESC, priority DESC
