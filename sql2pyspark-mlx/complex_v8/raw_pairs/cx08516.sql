WITH cleaned_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        channel
    FROM tickets
),
with_children_events AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_type,
        t.event_at
    FROM cleaned_tickets s
    JOIN ticket_events t ON t.ticket_id = s.id AND t.event_type IN ('assigned', 'replied')
),
enriched AS (
    -- Add calculated columns
    SELECT *, LOWER(TRIM(event_type)) AS event_type_clean
    FROM with_children_events
),
enriched_agent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.event_id,
        s.event_type_clean,
        su.tier AS agent_tier,
        su.hired_on AS agent_hired_on,
        su.agent_name
    FROM enriched s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
with_stats AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM tickets t WHERE t.agent_id = s.agent_id) AS ticket_count,
        (SELECT MAX(t.opened_at) FROM tickets t WHERE t.agent_id = s.agent_id) AS last_ticket_opened_at,
        (SELECT COUNT(CASE WHEN t.channel = 'email' THEN 1 END) FROM tickets t WHERE t.agent_id = s.agent_id) AS ticket_channel_count
    FROM enriched_agent s
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SIZE(COLLECT_SET(agent_tier) OVER (PARTITION BY ticket_channel_count, ticket_count)) AS distinct_agent_tier_in_ticket_channel_count_tick,
        DENSE_RANK() OVER w_agent_hired_on_month_ticket AS agent_hired_on_dense_rank,
        FIRST_VALUE(last_ticket_opened_at) IGNORE NULLS OVER (ORDER BY ticket_channel_count ASC, id, event_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_last_ticket_opened_at
    FROM with_stats
    WINDOW w_agent_hired_on_month_ticket AS (PARTITION BY DATE_TRUNC('MONTH', agent_hired_on), ticket_count ORDER BY agent_hired_on DESC)
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.agent_id,
        s.event_id,
        s.event_type_clean,
        s.agent_tier,
        s.agent_hired_on,
        s.agent_name,
        s.ticket_channel_count,
        s.distinct_agent_tier_in_ticket_channel_count_tick,
        s.agent_hired_on_dense_rank,
        t.actor AS event_actor,
        t.ticket_id AS event_ticket_id
    FROM with_trends s
    JOIN ticket_events t ON t.id = s.event_id
    WHERE t.actor IN ('system', 'customer')
)
-- Final projection
SELECT
    id,
    event_id,
    agent_tier,
    agent_hired_on,
    agent_name,
    distinct_agent_tier_in_ticket_channel_count_tick,
    DATEDIFF(DATE '2025-06-30', agent_hired_on) AS days_since_agent_hired_on,
    LOWER(REGEXP_REPLACE(TRIM(agent_tier), ' ', '_')) AS agent_tier_slug
FROM combined_details
ORDER BY agent_name DESC, id, event_id
LIMIT 10
