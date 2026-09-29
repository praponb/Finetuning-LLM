WITH prepared AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags
    FROM tickets
    WHERE opened_at < TIMESTAMP '2025-03-02 09:00:00'
),
combined_details AS (
    -- Attach agent attributes
    SELECT
        s.*,
        su.tier AS agent_tier,
        su.team AS agent_team,
        su.hired_on AS agent_hired_on
    FROM prepared s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
combined_details_2 AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.channel,
        s.csat,
        s.tags,
        s.agent_tier,
        s.agent_team,
        s.agent_hired_on,
        su.tier AS agent_tier_2,
        su.agent_name,
        su.hired_on AS agent_hired_on_2
    FROM combined_details s
    JOIN support_agents su ON su.id = s.agent_id AND su.hired_on IS NOT NULL
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT
        agent_id,
        MAX(requester_id) AS max_ticket_requester_id,
        COUNT(*) AS ticket_count
    FROM tickets
    GROUP BY agent_id
),
with_stats AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        t.max_ticket_requester_id,
        COALESCE(t.ticket_count, 0) AS ticket_count
    FROM combined_details_2 s
    LEFT JOIN ticket_stats t ON t.agent_id = s.agent_id
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN agent_tier_2 IN ('T3', 'T1') THEN 'group_a' WHEN agent_tier_2 = 'T2' THEN 'group_b' ELSE 'other' END AS agent_tier_2_group,
        ROUND(max_ticket_requester_id / NULLIF(csat, 0), 3) AS max_ticket_requester_id_per_csat,
        SUBSTRING(TRIM(agent_tier_2), 1, 3) AS agent_tier_2_prefix
    FROM with_stats
    WHERE YEAR(agent_hired_on) = 2024
),
with_parent AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.csat,
        s.agent_hired_on,
        s.agent_tier_2,
        s.agent_name,
        s.agent_hired_on_2,
        s.max_ticket_requester_id,
        s.ticket_count,
        s.agent_tier_2_prefix,
        su.tier AS agent_tier_3,
        su.hired_on AS agent_hired_on_3
    FROM derived s
    LEFT JOIN support_agents su ON su.id = s.agent_id
)
-- Final projection
SELECT
    id,
    agent_id,
    csat,
    agent_hired_on,
    agent_name,
    ticket_count,
    agent_tier_2_prefix,
    agent_tier_3,
    agent_hired_on_3,
    CASE WHEN csat >= 3 THEN 'A' WHEN csat >= 2 THEN 'B' ELSE 'C' END AS csat_tier,
    DATE_TRUNC('YEAR', agent_hired_on) AS agent_hired_on_year
FROM with_parent
