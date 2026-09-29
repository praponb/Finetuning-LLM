WITH prepared AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags
    FROM tickets
    WHERE csat <= 4
),
unique_rows AS (
    -- Keep the earliest row per agent_id
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY agent_id ORDER BY closed_at, id) AS rn
        FROM prepared
    ) ranked
    WHERE rn = 1
),
with_counts AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM tickets t WHERE t.agent_id = s.agent_id) AS ticket_count,
        (SELECT MIN(t.closed_at) FROM tickets t WHERE t.agent_id = s.agent_id) AS first_ticket_closed_at,
        (SELECT MIN(t.opened_at) FROM tickets t WHERE t.agent_id = s.agent_id) AS first_ticket_opened_at
    FROM unique_rows s
),
detail_rows_events AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.channel,
        s.csat,
        s.first_ticket_opened_at,
        t.id AS event_id,
        t.event_at,
        t.actor AS event_actor
    FROM with_counts s
    LEFT JOIN ticket_events t ON t.ticket_id = s.id AND t.actor IS NOT NULL
),
latest_per_key AS (
    -- Keep the earliest row per agent_id, channel
    SELECT
        id,
        agent_id,
        closed_at,
        channel,
        csat,
        first_ticket_opened_at,
        event_id,
        event_at,
        event_actor
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY agent_id, channel ORDER BY event_at, id, event_id) AS recency_rank
        FROM detail_rows_events
    ) ranked
    WHERE recency_rank = 1
),
detail_rows AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.channel,
        s.csat,
        s.first_ticket_opened_at,
        s.event_id,
        s.event_at,
        s.event_actor,
        t.id AS event_id_2,
        t.actor AS event_actor_2,
        t.event_at AS event_event_at
    FROM latest_per_key s
    JOIN ticket_events t ON t.ticket_id = s.id AND HOUR(t.event_at) BETWEEN 7 AND 11
)
-- Final projection
SELECT
    id,
    closed_at,
    channel,
    csat,
    first_ticket_opened_at,
    event_id,
    event_at,
    event_actor,
    event_id_2,
    event_event_at,
    event_at + INTERVAL 30 MINUTES AS event_at_plus_30m
FROM detail_rows
ORDER BY event_id DESC, id, event_id_2
LIMIT 5
