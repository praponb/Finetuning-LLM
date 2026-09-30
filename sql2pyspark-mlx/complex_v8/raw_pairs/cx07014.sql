WITH base AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LENGTH(TRIM(event_type)) AS event_type_len,
        CONCAT(event_type, '#', CAST(ticket_id AS STRING)) AS event_type_ref
    FROM ticket_events
    WHERE actor = 'system'
      AND event_type IS NOT NULL
),
combined_details AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.event_type_len,
        s.event_type_ref,
        t.tags AS ticket_tags,
        t.channel AS ticket_channel,
        t.priority AS ticket_priority
    FROM base s
    JOIN tickets t ON t.id = s.ticket_id
),
combined_details_2 AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.event_type_len,
        s.event_type_ref,
        s.ticket_tags,
        s.ticket_channel,
        s.ticket_priority,
        t.opened_at AS ticket_opened_at,
        t.agent_id AS ticket_agent_id,
        su.tier AS agent_tier,
        su.agent_name
    FROM combined_details s
    JOIN tickets t ON t.id = s.ticket_id
    JOIN support_agents su ON su.id = t.agent_id
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT(event_type_ref, '#', CAST(ticket_agent_id AS STRING)) AS event_type_ref_ref,
        DATEDIFF(DATE '2025-12-31', ticket_opened_at) AS days_since_ticket_opened_at
    FROM combined_details_2
    WHERE ticket_channel = 'phone'
),
exploded AS (
    -- One row per ticket_tag
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.event_type_len,
        s.event_type_ref,
        s.ticket_channel,
        s.ticket_priority,
        s.ticket_opened_at,
        s.ticket_agent_id,
        s.agent_tier,
        s.agent_name,
        s.event_type_ref_ref,
        s.days_since_ticket_opened_at,
        t.ticket_tag
    FROM tagged s
    LATERAL VIEW OUTER EXPLODE(s.ticket_tags) t AS ticket_tag
),
latest_per_key AS (
    -- Keep the earliest row per ticket_priority
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_type_len,
        event_type_ref,
        ticket_channel,
        ticket_priority,
        ticket_opened_at,
        ticket_agent_id,
        agent_tier,
        agent_name,
        event_type_ref_ref,
        days_since_ticket_opened_at,
        ticket_tag
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY ticket_priority ORDER BY days_since_ticket_opened_at, id, ticket_tag) AS rn
        FROM exploded
    ) ranked
    WHERE rn = 1
),
with_activity AS (
    -- Compare against the overall max
    SELECT *
    FROM latest_per_key
    WHERE days_since_ticket_opened_at < (SELECT MAX(days_since_ticket_opened_at) FROM latest_per_key)
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT event_type_len, agent_tier, ticket_channel, days_since_ticket_opened_at
    FROM with_activity
) src
PIVOT (
    AVG(days_since_ticket_opened_at) AS avg_v, SUM(days_since_ticket_opened_at) AS sum_v
    FOR ticket_channel IN ('web', 'phone', 'email', 'chat')
)
ORDER BY event_type_len, agent_tier
