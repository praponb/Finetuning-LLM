WITH base AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        channel,
        csat,
        tags,
        COALESCE(priority, 'UNKNOWN') AS priority_or_unknown,
        QUARTER(opened_at) AS opened_at_quarter,
        csat + requester_id AS csat_plus_requester_id
    FROM tickets
    WHERE ARRAY_CONTAINS(tags, 'refund')
),
exploded_detail_events AS (
    -- One row per event
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.priority,
        s.channel,
        s.csat,
        s.tags,
        s.priority_or_unknown,
        s.opened_at_quarter,
        t.id AS event_id,
        t.event_at,
        t.event_type
    FROM base s
    JOIN ticket_events t ON t.ticket_id = s.id
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        agent_id,
        channel,
        csat,
        tags,
        priority_or_unknown,
        opened_at_quarter,
        event_id,
        event_at,
        csat - requester_id AS csat_minus_requester_id
    FROM exploded_detail_events
    WHERE SIZE(tags) >= 2
),
detail_rows_events AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.channel,
        s.tags,
        s.opened_at_quarter,
        s.event_id,
        s.event_at,
        t.id AS event_id_2,
        t.event_at AS event_event_at,
        t.actor AS event_actor
    FROM tagged s
    JOIN ticket_events t ON t.ticket_id = s.id
),
per_label AS (
    -- One row per tag
    SELECT
        s.id,
        s.agent_id,
        s.channel,
        s.opened_at_quarter,
        s.event_id,
        s.event_at,
        s.event_id_2,
        s.event_event_at,
        s.event_actor,
        t.tag
    FROM detail_rows_events s
    LATERAL VIEW EXPLODE(s.tags) t AS tag
),
enriched AS (
    -- Attach event attributes
    SELECT
        s.id,
        s.agent_id,
        s.event_id,
        s.event_at,
        s.event_id_2,
        s.event_event_at,
        s.tag,
        t.actor AS event_actor_2,
        t.event_at AS event_event_at_2,
        su.tier AS agent_tier,
        su.agent_name
    FROM per_label s
    JOIN ticket_events t ON t.id = s.event_id_2
    JOIN support_agents su ON su.id = s.agent_id
)
SELECT
    id,
    agent_id,
    event_at,
    event_id_2,
    tag,
    event_actor_2,
    agent_tier,
    agent_name,
    DATE_FORMAT(event_event_at_2, 'yyyy-MM') AS event_event_at_2_label
FROM enriched
ORDER BY agent_id DESC
