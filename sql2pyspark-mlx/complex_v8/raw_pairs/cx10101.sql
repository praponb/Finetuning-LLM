WITH filtered AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_type NOT IN ('reopened', 'resolved')
),
with_parent AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.actor,
        t.channel AS ticket_channel,
        t.requester_id AS ticket_requester_id,
        su.tier AS agent_tier,
        su.agent_name
    FROM filtered s
    LEFT JOIN tickets t ON t.id = s.ticket_id
    JOIN support_agents su ON su.id = t.agent_id
),
with_activity AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM with_parent s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.actor = 'system'
        )
),
filtered_2 AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM with_activity s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
        )
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        ABS(ticket_requester_id - 18) AS ticket_requester_id_distance,
        LOWER(REGEXP_REPLACE(TRIM(ticket_channel), ' ', '_')) AS ticket_channel_slug,
        CONCAT(ticket_channel, '#', CAST(id AS STRING)) AS ticket_channel_ref
    FROM filtered_2
),
qualified AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM enriched s
    WHERE s.ticket_id IN (
            SELECT t.ticket_id
            FROM ticket_events t
        )
),
detail_rows AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.agent_name,
        s.ticket_channel_ref,
        t.id AS event_id,
        t.event_type,
        t.event_at,
        t.actor AS event_actor
    FROM qualified s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id
)
SELECT
    event_actor,
    event_id,
    GROUPING(event_actor) AS is_total_event_actor,
    COUNT(*) AS row_count
FROM detail_rows
GROUP BY GROUPING SETS ((event_actor, event_id), (event_actor), ())
ORDER BY event_actor NULLS LAST, event_id NULLS LAST
