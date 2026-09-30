WITH valid AS (
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
        UPPER(TRIM(channel)) AS channel_clean
    FROM tickets
    WHERE closed_at < TIMESTAMP '2025-02-27 06:00:00'
),
with_details_agent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.priority,
        s.csat,
        s.tags,
        su.agent_name,
        su.hired_on AS agent_hired_on,
        su.tier AS agent_tier
    FROM valid s
    JOIN support_agents su ON su.id = s.agent_id AND su.hired_on >= DATE '2024-07-28'
),
exploded_detail_events AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.priority,
        s.csat,
        s.agent_name,
        s.agent_hired_on,
        s.agent_tier,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_at,
        t.event_type
    FROM with_details_agent s
    JOIN ticket_events t ON t.ticket_id = s.id
)
-- Final projection
SELECT
    id,
    agent_id,
    opened_at,
    priority,
    csat,
    agent_name,
    agent_hired_on,
    agent_tier,
    event_id,
    event_actor,
    event_at,
    event_type,
    COALESCE(csat, 0) AS csat_filled,
    CASE WHEN csat >= 4 THEN 'HIGH' WHEN csat >= 3 THEN 'MEDIUM' ELSE 'LOW' END AS csat_tier
FROM exploded_detail_events
ORDER BY agent_id DESC
