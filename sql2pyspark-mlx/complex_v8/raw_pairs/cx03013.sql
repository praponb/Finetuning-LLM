WITH filtered_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM tickets
    WHERE agent_id IS NOT NULL
      AND priority NOT IN ('P4', 'P2')
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        GREATEST(requester_id, csat) AS greatest_requester_id_csat,
        SUBSTRING(TRIM(priority), 1, 2) AS priority_prefix,
        requester_id - csat AS requester_id_minus_csat
    FROM filtered_tickets
)
-- Subtotals with GROUPING SETS
SELECT
    channel,
    agent_id,
    MIN(csat) AS min_csat,
    SUM(requester_id_minus_csat) AS total_requester_id_minus_csat,
    COUNT(DISTINCT id) AS distinct_ids
FROM tagged
GROUP BY GROUPING SETS ((channel, agent_id), (channel), (agent_id), ())
ORDER BY channel NULLS LAST, agent_id NULLS LAST
