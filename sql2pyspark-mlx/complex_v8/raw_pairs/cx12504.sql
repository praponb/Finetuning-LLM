WITH scoped_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        CASE WHEN requester_id >= 22 THEN 'A' WHEN requester_id >= 6 THEN 'B' ELSE 'C' END AS requester_id_tier,
        CASE WHEN requester_id >= 21 THEN 'HIGH' WHEN requester_id >= 8 THEN 'MEDIUM' ELSE 'LOW' END AS requester_id_tier_2,
        LOWER(REGEXP_REPLACE(TRIM(channel), ' ', '_')) AS channel_slug
    FROM tickets
    WHERE (priority = 'P2' OR requester_id > 5)
),
detail_rows AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.priority,
        s.channel,
        s.csat,
        s.tags,
        s.requester_id_tier,
        s.requester_id_tier_2,
        t.id AS event_id,
        t.event_at,
        t.event_type
    FROM scoped_tickets s
    LEFT JOIN ticket_events t ON t.ticket_id = s.id AND t.event_type NOT IN ('replied', 'assigned')
),
joined AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.priority,
        s.csat,
        s.tags,
        s.requester_id_tier_2,
        s.event_id,
        s.event_at,
        su.team AS agent_team,
        su.hired_on AS agent_hired_on,
        su.tier AS agent_tier
    FROM detail_rows s
    LEFT JOIN support_agents su ON su.id = s.agent_id
)
SELECT
    id,
    tags,
    event_id,
    event_at,
    agent_team,
    agent_hired_on,
    agent_tier,
    ABS(csat - 3) AS csat_distance,
    CONCAT(priority, '#', CAST(id AS STRING)) AS priority_ref
FROM joined
ORDER BY agent_team, id DESC, event_id
LIMIT 20
