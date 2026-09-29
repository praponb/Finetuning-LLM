WITH base_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        channel,
        COALESCE(channel, 'UNKNOWN') AS channel_or_unknown
    FROM tickets
    WHERE channel IN ('email', 'web')
      AND csat >= 3
),
line_level_events AS (
    -- One row per event
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.priority,
        s.channel,
        s.channel_or_unknown,
        t.id AS event_id,
        t.event_type,
        t.actor AS event_actor
    FROM base_tickets s
    JOIN ticket_events t ON t.ticket_id = s.id
),
filtered AS (
    -- Keep rows whose event matches
    SELECT s.*
    FROM line_level_events s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.id = s.event_id
              AND HOUR(t.event_at) BETWEEN 8 AND 13
              AND t.event_type <> 'replied'
        )
),
latest_per_key AS (
    -- Keep the latest row per priority, event_type
    SELECT
        id,
        requester_id,
        agent_id,
        priority,
        channel,
        channel_or_unknown,
        event_id,
        event_type,
        event_actor
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY priority, event_type ORDER BY requester_id DESC, id DESC, event_id DESC) AS rn
        FROM filtered
    ) ranked
    WHERE rn = 1
)
-- Final projection
SELECT
    id,
    requester_id,
    agent_id,
    priority,
    event_id,
    event_actor,
    INITCAP(TRIM(event_actor)) AS event_actor_clean,
    COALESCE(requester_id, 0) AS requester_id_filled
FROM latest_per_key
ORDER BY event_actor_clean DESC
