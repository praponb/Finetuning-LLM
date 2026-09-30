WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        DATEDIFF(DATE '2025-06-30', closed_at) AS days_since_closed_at,
        CASE WHEN requester_id >= 15 THEN 'large' WHEN requester_id >= 8 THEN 'medium' ELSE 'small' END AS requester_id_tier
    FROM tickets
    WHERE (TO_DATE(closed_at) < DATE '2025-02-10' OR agent_id >= 5)
),
with_activity AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM prepared s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND ARRAY_CONTAINS(t.tags, 'refund')
              AND t.closed_at >= TIMESTAMP '2025-01-17 21:00:00'
        )
      AND s.opened_at >= TIMESTAMP '2025-01-13 12:00:00'
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN days_since_closed_at >= 490 THEN 'A' WHEN days_since_closed_at >= 210 THEN 'B' ELSE 'C' END AS days_since_closed_at_tier
    FROM with_activity
),
enriched AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.days_since_closed_at,
        s.requester_id_tier,
        s.days_since_closed_at_tier,
        su.team AS agent_team
    FROM scored s
    LEFT JOIN support_agents su ON su.id = s.agent_id
)
-- Subtotals with GROUPING SETS
SELECT
    agent_id,
    COALESCE(requester_id_tier, 'ALL') AS requester_id_tier,
    COALESCE(days_since_closed_at_tier, 'ALL') AS days_since_closed_at_tier,
    COUNT(DISTINCT days_since_closed_at_tier) AS distinct_days_since_closed_at_tiers,
    COUNT(*) AS row_count
FROM enriched
GROUP BY GROUPING SETS ((agent_id, requester_id_tier, days_since_closed_at_tier), (agent_id), ())
ORDER BY agent_id NULLS LAST, requester_id_tier NULLS LAST, days_since_closed_at_tier NULLS LAST
