WITH recent_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_FORMAT(event_at, 'yyyy-MM-dd HH:00') AS event_at_label,
        event_at + INTERVAL 7 DAYS AS event_at_plus_7d
    FROM ticket_events
    WHERE actor = 'system'
      AND event_type = 'created'
),
latest_per_key AS (
    -- Keep the latest row per ticket_id, event_type
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_at_label,
        event_at_plus_7d
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY ticket_id, event_type ORDER BY event_at DESC, id DESC) AS recency_rank
        FROM recent_events
    ) ranked
    WHERE recency_rank = 1
),
qualified AS (
    -- Keep rows whose ticket matches
    SELECT s.*
    FROM latest_per_key s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.id = s.ticket_id
              AND t.priority = 'P2'
        )
),
with_parent_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        s.event_at_label,
        t.agent_id AS ticket_agent_id,
        t.opened_at AS ticket_opened_at,
        t.channel AS ticket_channel
    FROM qualified s
    JOIN tickets t ON t.id = s.ticket_id
),
qualified_2 AS (
    -- Rows with no matching tickets
    SELECT s.*
    FROM with_parent_ticket s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.ticket_agent_id
              AND t.priority = 'P4'
              AND t.channel IS NOT NULL
        )
      AND s.event_type NOT IN ('assigned', 'replied')
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        ticket_id,
        event_type,
        actor,
        event_at_label,
        ticket_agent_id,
        ticket_opened_at,
        LENGTH(TRIM(event_type)) AS event_type_len,
        ticket_opened_at + INTERVAL 30 MINUTES AS ticket_opened_at_plus_30m,
        DATE_FORMAT(ticket_opened_at, 'yyyy-MM-dd HH:00') AS ticket_opened_at_label
    FROM qualified_2
)
SELECT
    id,
    ticket_id,
    actor,
    event_at_label,
    ticket_agent_id,
    ticket_opened_at,
    event_type_len,
    ticket_opened_at_plus_30m,
    ticket_opened_at_label,
    DAYOFWEEK(ticket_opened_at) AS ticket_opened_at_dayofweek,
    SUBSTRING(TRIM(event_type), 1, 3) AS event_type_prefix
FROM derived
WHERE actor = 'system'
ORDER BY ticket_opened_at, event_type_prefix DESC, id
LIMIT 20
