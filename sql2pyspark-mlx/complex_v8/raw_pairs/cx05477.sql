WITH base_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        CONCAT(channel, '#', CAST(csat AS STRING)) AS channel_ref,
        CONCAT_WS('|', channel, priority) AS channel_priority_label
    FROM tickets
    WHERE agent_id IS NOT NULL
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.closed_at,
        s.priority,
        s.channel_ref,
        s.channel_priority_label,
        su.agent_name,
        su.tier AS agent_tier
    FROM base_tickets s
    JOIN support_agents su ON su.id = s.agent_id
),
agent_id_stats AS (
    -- Max requester_id per agent_id
    SELECT agent_id, MAX(requester_id) AS max_requester_id
    FROM with_details
    GROUP BY agent_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_requester_id,
        ROUND(s.requester_id - g.max_requester_id, 2) AS requester_id_minus_max
    FROM with_details s
    JOIN agent_id_stats g ON g.agent_id = s.agent_id
    WHERE s.requester_id = g.max_requester_id
),
with_stats AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.id) AS last_event_at,
        (SELECT MIN(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.id) AS first_event_at
    FROM vs_group s
),
latest_per_key AS (
    -- Keep the latest row per priority, agent_tier
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel_ref,
        channel_priority_label,
        agent_name,
        agent_tier,
        max_requester_id,
        requester_id_minus_max,
        last_event_at,
        first_event_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY priority, agent_tier ORDER BY closed_at DESC, id DESC) AS rn
        FROM with_stats
    ) ranked
    WHERE rn = 1
),
with_activity AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM latest_per_key s
    WHERE s.id IN (
            SELECT t.ticket_id
            FROM ticket_events t
        )
      AND s.agent_tier IN ('T3', 'T2')
)
SELECT *
FROM (
    SELECT last_event_at, first_event_at, priority, requester_id_minus_max
    FROM with_activity
) src
PIVOT (
    COUNT(requester_id_minus_max)
    FOR priority IN ('P4', 'P1', 'P2', 'P3')
)
ORDER BY last_event_at, first_event_at
