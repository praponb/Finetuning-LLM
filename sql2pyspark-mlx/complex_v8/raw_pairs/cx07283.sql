WITH covered AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.agent_id,
        a.opened_at,
        COUNT(b.id) AS prior_rows_1d,
        SUM(b.requester_id) AS prior_requester_id_1d
    FROM tickets a
    LEFT JOIN tickets b
      ON b.agent_id = a.agent_id
     AND b.opened_at < a.opened_at
     AND b.opened_at >= a.opened_at - INTERVAL 1 DAYS
    GROUP BY a.id, a.agent_id, a.opened_at
),
without_matches AS (
    -- Rows with no matching tickets
    SELECT s.*
    FROM covered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND t.csat > 1
              AND t.priority = 'P1'
        )
)
SELECT
    prior_rows_1d,
    agent_id,
    MAX(opened_at) AS last_opened_at,
    MIN(opened_at) AS first_opened_at,
    COUNT(*) AS row_count
FROM without_matches
GROUP BY GROUPING SETS ((prior_rows_1d, agent_id), (prior_rows_1d), (agent_id), ())
ORDER BY prior_rows_1d NULLS LAST, agent_id NULLS LAST
