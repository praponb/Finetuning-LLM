WITH filtered AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        tags,
        SUBSTRING(TRIM(channel), 1, 2) AS channel_prefix
    FROM tickets
),
with_children_events AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.channel,
        s.channel_prefix,
        t.id AS event_id,
        t.event_type,
        t.actor AS event_actor,
        t.event_at
    FROM filtered s
    LEFT JOIN ticket_events t ON t.ticket_id = s.id
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY channel, event_id ORDER BY opened_at), 3) AS opened_at_percent_rank,
        RANK() OVER w_channel_event_id_by_opened_a_2 AS opened_at_rank
    FROM with_children_events
    WINDOW w_channel_event_id_by_opened_a AS (PARTITION BY channel, event_id ORDER BY opened_at), w_channel_event_id_by_opened_a_2 AS (PARTITION BY channel, event_id ORDER BY opened_at, id, event_id)
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MIN(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.id) AS first_event_at,
        (SELECT COUNT(*) FILTER (WHERE t.event_type = 'created') FROM ticket_events t WHERE t.ticket_id = s.id) AS event_event_type_count
    FROM sequenced s
),
combined_details AS (
    -- Attach event attributes
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.channel,
        s.channel_prefix,
        s.event_id,
        s.event_type,
        s.event_actor,
        s.event_at,
        s.opened_at_percent_rank,
        s.opened_at_rank,
        s.first_event_at,
        s.event_event_type_count,
        t.event_at AS event_event_at,
        t.event_type AS event_event_type
    FROM summarized_rows s
    JOIN ticket_events t ON t.id = s.event_id
),
joined AS (
    -- Join ticket_events
    SELECT
        s.id,
        s.agent_id,
        s.channel,
        s.event_id,
        s.event_type,
        s.event_actor,
        s.opened_at_percent_rank,
        s.opened_at_rank,
        s.first_event_at,
        s.event_event_type_count,
        t.event_type AS event_event_type_2
    FROM combined_details s
    LEFT JOIN ticket_events t ON t.id = s.event_id
),
with_details AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.agent_id,
        s.channel,
        s.event_id,
        s.event_type,
        s.event_actor,
        s.opened_at_percent_rank,
        s.opened_at_rank,
        s.first_event_at,
        s.event_event_type_count,
        s.event_event_type_2,
        su.agent_name,
        su.tier AS agent_tier,
        su.hired_on AS agent_hired_on
    FROM joined s
    JOIN support_agents su ON su.id = s.agent_id
)
-- Final projection
SELECT
    id,
    channel,
    event_id,
    event_type,
    event_actor,
    opened_at_percent_rank,
    opened_at_rank,
    event_event_type_2,
    agent_name,
    agent_tier,
    agent_hired_on,
    CONCAT_WS(' / ', agent_name, event_actor) AS agent_name_event_actor_label
FROM with_details
WHERE agent_id IS NOT NULL
ORDER BY event_actor DESC
