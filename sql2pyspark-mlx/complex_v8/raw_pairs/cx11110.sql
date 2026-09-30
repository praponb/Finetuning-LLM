WITH matched AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.agent_id,
        a.opened_at,
        COUNT(b.id) AS prior_rows_7d,
        SUM(b.requester_id) AS prior_requester_id_7d
    FROM tickets a
    LEFT JOIN tickets b
      ON b.agent_id = a.agent_id
     AND b.opened_at < a.opened_at
     AND b.opened_at >= a.opened_at - INTERVAL 7 DAYS
    GROUP BY a.id, a.agent_id, a.opened_at
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        TO_DATE(opened_at) AS opened_at_date,
        ABS(prior_rows_7d - 3) AS prior_rows_7d_distance,
        COALESCE(prior_rows_7d, 0) AS prior_rows_7d_filled
    FROM matched
)
SELECT
    id,
    agent_id,
    opened_at,
    prior_rows_7d,
    prior_requester_id_7d,
    opened_at_date,
    prior_rows_7d_filled,
    ROUND(prior_requester_id_7d / NULLIF(prior_rows_7d_filled, 0), 3) AS prior_requester_id_7d_per_prior_rows_7d_filled
FROM labeled
ORDER BY prior_rows_7d_filled DESC, prior_requester_id_7d_per_prior_rows_7d_filled DESC
