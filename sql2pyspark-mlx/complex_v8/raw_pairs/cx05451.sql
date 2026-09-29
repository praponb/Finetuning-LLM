WITH scoped_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        csat,
        tags,
        opened_at + INTERVAL 15 MINUTES AS opened_at_plus_15m,
        CONCAT_WS(' / ', channel, priority) AS channel_priority_label
    FROM tickets
    WHERE priority IN ('P2', 'P1', 'P4')
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.priority,
        s.tags,
        s.opened_at_plus_15m,
        s.channel_priority_label,
        su.tier AS agent_tier
    FROM scoped_tickets s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
agent_tier_stats AS (
    -- Median requester_id per agent_tier
    SELECT agent_tier, PERCENTILE_APPROX(requester_id, 0.5) AS median_requester_id
    FROM with_parent
    GROUP BY agent_tier
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_requester_id,
        ROUND(s.requester_id - g.median_requester_id, 2) AS requester_id_minus_median
    FROM with_parent s
    JOIN agent_tier_stats g ON g.agent_tier = s.agent_tier
    WHERE s.requester_id >= g.median_requester_id
),
detail_rows AS (
    -- One row per event
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.priority,
        s.opened_at_plus_15m,
        s.channel_priority_label,
        s.median_requester_id,
        s.requester_id_minus_median,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_at
    FROM vs_group s
    LEFT JOIN ticket_events t ON t.ticket_id = s.id
)
SELECT
    id,
    requester_id,
    agent_id,
    priority,
    opened_at_plus_15m,
    requester_id_minus_median,
    event_id,
    event_actor,
    event_at,
    COALESCE(priority, 'UNKNOWN') AS priority_or_unknown,
    DATEDIFF(DATE '2025-12-31', event_at) AS days_since_event_at
FROM detail_rows
ORDER BY days_since_event_at DESC, id, event_id
LIMIT 5
