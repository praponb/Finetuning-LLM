WITH prepared_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        csat,
        tags
    FROM tickets
    WHERE channel IN ('email', 'web', 'chat')
),
enriched_agent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.csat,
        s.tags,
        su.tier AS agent_tier,
        su.agent_name,
        su.hired_on AS agent_hired_on
    FROM prepared_tickets s
    LEFT JOIN support_agents su ON su.id = s.agent_id AND su.agent_name IS NOT NULL
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        csat,
        tags,
        agent_tier,
        agent_hired_on,
        SUBSTRING(TRIM(agent_tier), 1, 2) AS agent_tier_prefix,
        ABS(csat - 3) AS csat_distance,
        SUBSTRING(TRIM(agent_tier), 1, 4) AS agent_tier_prefix_2
    FROM enriched_agent
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(csat) OVER w_csat, 2) AS csat_avg_csat,
        COUNT(*) OVER (PARTITION BY csat) AS rows_in_csat,
        MAX(csat) OVER (PARTITION BY csat) AS csat_max_csat,
        LAST_VALUE(requester_id) OVER (PARTITION BY DATE_TRUNC('MONTH', opened_at), agent_id ORDER BY agent_hired_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_requester_id
    FROM labeled
    WINDOW w_csat AS (PARTITION BY csat)
),
with_children_events AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.agent_tier,
        s.agent_hired_on,
        s.csat_distance,
        s.agent_tier_prefix_2,
        s.csat_avg_csat,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_at,
        t.event_type
    FROM ranked s
    JOIN ticket_events t ON t.ticket_id = s.id
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        COUNT(*) FILTER (WHERE actor = 'system') AS event_actor_count,
        COUNT(*) FILTER (WHERE actor IS NOT NULL) AS event_actor_count_2,
        MAX(event_at) AS last_event_at
    FROM ticket_events
    GROUP BY ticket_id
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        COALESCE(t.event_actor_count, 0) AS event_actor_count,
        COALESCE(t.event_actor_count_2, 0) AS event_actor_count_2,
        t.last_event_at
    FROM with_children_events s
    LEFT JOIN event_stats t ON t.ticket_id = s.id
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        opened_at,
        csat_distance,
        agent_tier_prefix_2,
        csat_avg_csat,
        event_id,
        event_actor,
        event_at,
        event_type,
        event_actor_count_2,
        last_event_at,
        CASE WHEN event_actor IN ('agent', 'customer') THEN 'group_a' WHEN event_actor = 'system' THEN 'group_b' ELSE 'other' END AS event_actor_group,
        ABS(event_actor_count_2 - 4) AS event_actor_count_2_distance,
        COALESCE(event_actor_count_2, 0) AS event_actor_count_2_filled
    FROM summarized_rows
    WHERE event_actor_count > 1
)
-- Final projection
SELECT
    id,
    opened_at,
    csat_distance,
    event_id,
    event_actor,
    event_at,
    event_actor_group,
    event_actor_count_2_distance,
    event_actor_count_2_filled,
    GREATEST(event_actor_count_2, csat_distance) AS greatest_event_actor_count_2_csat_distance
FROM tagged
ORDER BY csat_distance NULLS LAST, id, event_id
LIMIT 10
