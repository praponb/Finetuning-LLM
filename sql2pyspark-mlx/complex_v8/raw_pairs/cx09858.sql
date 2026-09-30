WITH scoped_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        CASE WHEN csat >= 5 THEN 'HIGH' WHEN csat >= 2 THEN 'MEDIUM' ELSE 'LOW' END AS csat_tier
    FROM tickets
),
qualified AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM scoped_tickets s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND t.requester_id BETWEEN 2 AND 23
        )
),
enriched_agent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.channel,
        s.csat,
        s.tags,
        s.csat_tier,
        su.tier AS agent_tier,
        su.hired_on AS agent_hired_on
    FROM qualified s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE su.tier NOT IN ('T2', 'T1')
)
-- Subtotals with GROUPING SETS
SELECT
    channel,
    csat_tier,
    csat,
    GROUPING(channel) AS is_total_channel,
    COUNT(DISTINCT id) AS distinct_ids,
    ARRAY_SORT(COLLECT_SET(agent_tier)) AS agent_tier_list,
    MIN(csat) AS min_csat
FROM enriched_agent
GROUP BY GROUPING SETS ((channel, csat_tier, csat), (channel))
ORDER BY channel NULLS LAST, csat_tier NULLS LAST, csat NULLS LAST
