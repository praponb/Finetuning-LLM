WITH covered AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.agent_id,
        a.opened_at,
        COUNT(b.id) AS prior_rows_12h,
        SUM(b.requester_id) AS prior_requester_id_12h
    FROM tickets a
    LEFT JOIN tickets b
      ON b.agent_id = a.agent_id
     AND b.opened_at < a.opened_at
     AND b.opened_at >= a.opened_at - INTERVAL 12 HOURS
    GROUP BY a.id, a.agent_id, a.opened_at
),
without_matches AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM covered s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
        )
      AND s.agent_id IS NOT NULL
)
SELECT
    id,
    agent_id,
    opened_at,
    prior_rows_12h,
    prior_requester_id_12h,
    DATEDIFF(DATE '2025-12-31', opened_at) AS days_since_opened_at,
    DATE_FORMAT(opened_at, 'MMM yyyy') AS opened_at_label
FROM without_matches
