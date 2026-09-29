WITH covered AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.agent_id,
        a.opened_at,
        COUNT(b.id) AS prior_rows_7d,
        SUM(b.csat) AS prior_csat_7d
    FROM tickets a
    LEFT JOIN tickets b
      ON b.agent_id = a.agent_id
     AND b.opened_at < a.opened_at
     AND b.opened_at >= a.opened_at - INTERVAL 7 DAYS
    GROUP BY a.id, a.agent_id, a.opened_at
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        agent_id,
        prior_rows_7d,
        prior_csat_7d,
        LEAST(prior_csat_7d, prior_rows_7d) AS least_prior_csat_7d_prior_rows_7d,
        ABS(prior_csat_7d - 14) AS prior_csat_7d_distance,
        DATE_FORMAT(opened_at, 'yyyy-MM') AS opened_at_label
    FROM covered
    WHERE prior_csat_7d > 5
)
-- Final projection
SELECT id, prior_rows_7d
FROM labeled
WHERE prior_csat_7d < 16
ORDER BY prior_rows_7d DESC, id DESC
