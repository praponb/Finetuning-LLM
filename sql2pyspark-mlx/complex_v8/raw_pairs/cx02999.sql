WITH scoped AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        YEAR(closed_at) AS closed_at_year,
        SUBSTRING(TRIM(priority), 1, 3) AS priority_prefix,
        LENGTH(TRIM(priority)) AS priority_len
    FROM tickets
    WHERE opened_at < TIMESTAMP '2025-02-18 11:00:00'
),
joined AS (
    -- Join support_agents
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.priority,
        s.closed_at_year,
        s.priority_prefix,
        s.priority_len,
        su.tier AS agent_tier
    FROM scoped s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
deduped AS (
    -- Keep the latest row per agent_tier
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        closed_at_year,
        priority_prefix,
        priority_len,
        agent_tier
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY agent_tier ORDER BY priority_len DESC, id DESC) AS rn
        FROM joined
    ) ranked
    WHERE rn = 1
),
with_counts AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MIN(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.id) AS first_event_at,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.id) AS last_event_at,
        (SELECT MIN(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.id) AS first_event_at_2
    FROM deduped s
),
with_children AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.priority,
        s.priority_prefix,
        s.priority_len,
        s.agent_tier,
        s.first_event_at_2,
        t.id AS event_id,
        t.event_at,
        t.event_type,
        t.actor AS event_actor
    FROM with_counts s
    JOIN ticket_events t ON t.ticket_id = s.id
)
SELECT
    id,
    agent_id,
    opened_at,
    priority,
    priority_prefix,
    priority_len,
    event_id,
    event_at,
    event_type,
    event_actor,
    CONCAT_WS(': ', priority_prefix, event_actor) AS priority_prefix_event_actor_label,
    opened_at + INTERVAL 15 MINUTES AS opened_at_plus_15m
FROM with_children
ORDER BY priority_len DESC NULLS FIRST
