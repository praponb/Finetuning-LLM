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
        tags
    FROM tickets
    WHERE SIZE(tags) >= 1
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        RANK() OVER (PARTITION BY csat ORDER BY closed_at DESC, id) AS closed_at_rank,
        SUM(requester_id) OVER (PARTITION BY csat ORDER BY closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_requester_id,
        SUM(csat) OVER (PARTITION BY agent_id) AS agent_id_sum_csat
    FROM valid
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT(priority, '#', CAST(agent_id AS STRING)) AS priority_ref,
        opened_at + INTERVAL 2 HOURS AS opened_at_plus_2h
    FROM with_trends
)
-- Subtotals with ROLLUP
SELECT
    priority,
    agent_id,
    closed_at_rank,
    SUM(CASE WHEN priority NOT IN ('P1', 'P2') THEN csat ELSE 0 END) AS csat_priority_not_in_p1_p2
FROM scored
GROUP BY ROLLUP(priority, agent_id, closed_at_rank)
ORDER BY priority NULLS LAST, agent_id NULLS LAST, closed_at_rank NULLS LAST
