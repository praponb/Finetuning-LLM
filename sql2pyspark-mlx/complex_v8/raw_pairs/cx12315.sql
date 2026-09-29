WITH recent_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug
    FROM ticket_events
    WHERE (event_at >= TIMESTAMP '2025-02-06 11:00:00' OR event_type NOT IN ('reopened', 'escalated'))
),
enriched AS (
    -- Join tickets
    SELECT
        s.*,
        t.agent_id AS ticket_agent_id,
        t.channel AS ticket_channel,
        su.tier AS agent_tier,
        su.agent_name
    FROM recent_events s
    JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY ticket_id, ticket_agent_id ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at
    FROM enriched
)
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    actor,
    actor_slug,
    agent_tier,
    agent_name,
    CASE WHEN event_type IN ('created', 'replied') THEN 'group_a' WHEN event_type = 'reopened' THEN 'group_b' ELSE 'other' END AS event_type_group,
    LENGTH(TRIM(actor_slug)) AS actor_slug_len
FROM with_trends
