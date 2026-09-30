WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        UPPER(TRIM(event_type)) AS event_type_clean,
        CASE WHEN event_type IN ('assigned', 'reopened') THEN 'group_a' WHEN event_type = 'resolved' THEN 'group_b' ELSE 'other' END AS event_type_group
    FROM ticket_events
    WHERE (actor IN ('system', 'customer') OR ticket_id >= 5)
),
with_children AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.event_type_clean,
        t.id AS event_id,
        t.event_type AS event_event_type,
        t.actor AS event_actor,
        t.event_at AS event_event_at
    FROM cleaned s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id AND t.event_type IN ('escalated', 'replied')
),
with_activity AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM with_children s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
        )
      AND s.ticket_id IS NOT NULL
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        COUNT(*) AS event_count,
        COUNT(CASE WHEN event_type IS NOT NULL THEN 1 END) AS event_event_type_count
    FROM ticket_events
    GROUP BY ticket_id
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        COALESCE(t.event_count, 0) AS event_count,
        COALESCE(t.event_event_type_count, 0) AS event_event_type_count
    FROM with_activity s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
),
with_parent_event AS (
    -- Join ticket_events
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.event_type_clean,
        s.event_id,
        s.event_event_type,
        s.event_actor,
        s.event_event_at,
        t.ticket_id AS event_ticket_id,
        ti.priority AS ticket_priority,
        ti.opened_at AS ticket_opened_at
    FROM summarized_rows s
    JOIN ticket_events t ON t.id = s.event_id
    JOIN tickets ti ON ti.id = t.ticket_id
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        CONCAT(event_event_type, '#', CAST(event_id AS STRING)) AS event_event_type_ref,
        ROUND((UNIX_TIMESTAMP(event_event_at) - UNIX_TIMESTAMP(ticket_opened_at)) / 60, 1) AS minutes_ticket_opened_at_to_event_event_at
    FROM with_parent_event
    WHERE event_at < TIMESTAMP '2025-02-24 13:00:00'
),
combined_details AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.event_id,
        s.event_event_type,
        s.event_actor,
        s.event_event_at,
        s.event_ticket_id,
        s.ticket_priority,
        s.event_event_type_ref,
        s.minutes_ticket_opened_at_to_event_event_at,
        t.priority AS ticket_priority_2,
        t.agent_id AS ticket_agent_id,
        t.csat AS ticket_csat
    FROM labeled s
    JOIN tickets t ON t.id = s.event_ticket_id
    WHERE t.agent_id IS NOT NULL
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_event_type,
    event_actor,
    event_event_at,
    event_ticket_id,
    ticket_priority,
    event_event_type_ref,
    minutes_ticket_opened_at_to_event_event_at,
    ticket_agent_id,
    ticket_csat,
    LENGTH(TRIM(event_event_type)) AS event_event_type_len
FROM combined_details
WHERE event_event_type NOT IN ('created', 'escalated')
ORDER BY ticket_id DESC
