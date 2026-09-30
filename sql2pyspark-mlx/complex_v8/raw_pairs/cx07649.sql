WITH recent AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        UPPER(TRIM(actor)) AS actor_clean,
        LENGTH(TRIM(event_type)) AS event_type_len
    FROM ticket_events
    WHERE event_type = 'created'
),
with_details_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        t.channel AS ticket_channel,
        t.agent_id AS ticket_agent_id,
        t.requester_id AS ticket_requester_id
    FROM recent s
    JOIN tickets t ON t.id = s.ticket_id
    WHERE t.channel IS NOT NULL
),
line_level_events AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.actor_clean,
        s.ticket_channel,
        s.ticket_agent_id,
        t.id AS event_id,
        t.event_at AS event_event_at,
        t.actor AS event_actor,
        t.event_type AS event_event_type
    FROM with_details_ticket s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT agent_id, MIN(closed_at) AS first_ticket_closed_at
    FROM tickets
    GROUP BY agent_id
),
with_counts AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        t.first_ticket_closed_at
    FROM line_level_events s
    LEFT JOIN ticket_stats t ON t.agent_id = s.ticket_agent_id
),
with_stats AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_count,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_count_2
    FROM with_counts s
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        ROUND(event_count_2 / NULLIF(event_count, 0), 3) AS event_count_2_per_event_count,
        CONCAT_WS(' / ', event_event_type, actor_clean) AS event_event_type_actor_clean_label
    FROM with_stats
),
screened AS (
    -- Rows with no matching tickets
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.ticket_agent_id
              AND ARRAY_CONTAINS(t.tags, 'refund')
              AND t.opened_at >= TIMESTAMP '2025-01-19 10:00:00'
        )
)
SELECT
    id,
    ticket_id,
    ticket_channel,
    ticket_agent_id,
    event_id,
    event_event_at,
    event_event_type,
    first_ticket_closed_at,
    event_count,
    event_count_2,
    event_count_2_per_event_count
FROM screened
ORDER BY first_ticket_closed_at, event_event_type DESC NULLS FIRST, id, event_id
LIMIT 5
