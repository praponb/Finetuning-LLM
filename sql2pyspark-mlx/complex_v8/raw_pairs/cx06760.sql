WITH base_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        priority,
        channel,
        csat,
        csat - requester_id AS csat_minus_requester_id
    FROM tickets
    WHERE requester_id <= 17
      AND channel IN ('email', 'phone', 'chat')
),
qualified AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM base_tickets s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND t.csat < 4
              AND t.priority IN ('P4', 'P1')
        )
),
per_group AS (
    -- Summarise per group
    SELECT
        agent_id,
        channel,
        COUNT(DISTINCT id) AS distinct_ids,
        SUM(csat) AS total_csat,
        SUM(requester_id) AS total_requester_id,
        SUM(CASE WHEN priority NOT IN ('P2', 'P3') THEN csat ELSE 0 END) AS csat_priority_not_in_p2_p3,
        MAX(csat_minus_requester_id) AS max_csat_minus_requester_id,
        SUM(CASE WHEN channel IS NOT NULL THEN 1 ELSE 0 END) AS n_channel_is_not_null
    FROM qualified
    WHERE csat BETWEEN 3 AND 4
    GROUP BY 1, 2
)
-- Subtotals with ROLLUP
SELECT
    agent_id,
    distinct_ids,
    SUM(max_csat_minus_requester_id) AS total_max_csat_minus_requester_id,
    SUM(distinct_ids) AS total_distinct_ids
FROM per_group
GROUP BY ROLLUP(agent_id, distinct_ids)
