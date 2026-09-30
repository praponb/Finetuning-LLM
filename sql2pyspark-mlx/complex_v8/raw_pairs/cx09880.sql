WITH valid_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        channel,
        csat,
        ROUND(requester_id / NULLIF(csat, 0), 3) AS requester_id_per_csat,
        TO_DATE(opened_at) AS opened_at_date
    FROM tickets
    WHERE agent_id <= 5
),
enriched AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.csat,
        s.requester_id_per_csat,
        su.hired_on AS agent_hired_on
    FROM valid_tickets s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
joined_agent AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.csat,
        s.agent_hired_on,
        su.agent_name,
        su.tier AS agent_tier
    FROM enriched s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE su.tier IN ('T3', 'T2')
),
annotated AS (
    -- Derive helper columns
    SELECT *, LENGTH(TRIM(agent_tier)) AS agent_tier_len
    FROM joined_agent
),
joined_agent_2 AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.agent_id,
        s.csat,
        s.agent_hired_on,
        s.agent_tier,
        s.agent_tier_len,
        su.tier AS agent_tier_2,
        su.agent_name AS agent_agent_name
    FROM annotated s
    JOIN support_agents su ON su.id = s.agent_id
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        csat,
        agent_hired_on,
        agent_tier,
        agent_tier_len,
        agent_agent_name,
        UPPER(TRIM(agent_agent_name)) AS agent_agent_name_clean
    FROM joined_agent_2
)
-- Subtotals with CUBE
SELECT
    agent_tier,
    csat,
    SUM(csat) FILTER (WHERE UPPER(TRIM(agent_agent_name)) LIKE 'J%') AS csat_upper_trim_agent_agent_name,
    MAX(csat) AS max_csat
FROM labeled
GROUP BY CUBE(agent_tier, csat)
ORDER BY agent_tier NULLS LAST, csat NULLS LAST
