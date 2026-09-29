WITH recent_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT(actor, '#', CAST(id AS STRING)) AS actor_ref,
        CONCAT(event_type, '#', CAST(id AS STRING)) AS event_type_ref
    FROM ticket_events
),
with_activity AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM recent_events s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.event_type NOT IN ('replied', 'assigned')
              AND t.event_at >= TIMESTAMP '2025-01-13 11:00:00'
        )
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.actor_ref,
        s.event_type_ref,
        t.agent_id AS ticket_agent_id,
        t.channel AS ticket_channel,
        t.closed_at AS ticket_closed_at
    FROM with_activity s
    JOIN tickets t ON t.id = s.ticket_id
),
line_level AS (
    -- One row per ticket
    SELECT
        s.id,
        s.ticket_id,
        s.actor,
        s.event_type_ref,
        s.ticket_agent_id,
        s.ticket_channel,
        t.id AS ticket_id_2,
        t.closed_at AS ticket_closed_at_2,
        t.csat AS ticket_csat,
        t.tags AS ticket_tags,
        t.opened_at AS ticket_opened_at
    FROM joined s
    LEFT JOIN tickets t ON t.agent_id = s.ticket_agent_id
),
rollup_base AS (
    -- Summarise per group
    SELECT
        actor,
        ARRAY_SORT(COLLECT_SET(ticket_channel)) AS ticket_channel_list,
        MIN(ticket_opened_at) AS first_ticket_opened_at,
        MAX(ticket_opened_at) AS last_ticket_opened_at
    FROM line_level
    GROUP BY actor
    HAVING AVG(ticket_csat) > 1
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        DATE_FORMAT(first_ticket_opened_at, 'yyyy-MM-dd') AS first_ticket_opened_at_label,
        SIZE(ticket_channel_list) AS ticket_channel_list_count
    FROM rollup_base
),
per_label AS (
    -- One row per ticket_channel_list_item
    SELECT
        s.actor,
        s.first_ticket_opened_at,
        s.last_ticket_opened_at,
        s.first_ticket_opened_at_label,
        s.ticket_channel_list_count,
        t.ticket_channel_list_item
    FROM enriched s
    LATERAL VIEW EXPLODE(s.ticket_channel_list) t AS ticket_channel_list_item
)
SELECT
    actor,
    first_ticket_opened_at,
    last_ticket_opened_at,
    first_ticket_opened_at_label,
    ticket_channel_list_count,
    ticket_channel_list_item,
    SUBSTRING(TRIM(actor), 1, 4) AS actor_prefix,
    LENGTH(TRIM(ticket_channel_list_item)) AS ticket_channel_list_item_len
FROM per_label
