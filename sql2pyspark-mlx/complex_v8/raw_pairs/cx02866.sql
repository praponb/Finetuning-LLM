WITH joined_ranges AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.agent_id,
        a.opened_at,
        COUNT(b.id) AS prior_rows_3d,
        SUM(b.csat) AS prior_csat_3d
    FROM tickets a
    LEFT JOIN tickets b
      ON b.agent_id = a.agent_id
     AND b.opened_at < a.opened_at
     AND b.opened_at >= a.opened_at - INTERVAL 3 DAYS
    GROUP BY a.id, a.agent_id, a.opened_at
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_agent_id_by_prior_rows_3d, 3) AS prior_rows_3d_percent_rank,
        MIN(prior_rows_3d) OVER w_agent_id AS agent_id_min_prior_rows_3d
    FROM joined_ranges
    WINDOW w_agent_id_by_prior_rows_3d AS (PARTITION BY agent_id ORDER BY prior_rows_3d, id), w_agent_id AS (PARTITION BY agent_id)
)
-- Final projection
SELECT
    id,
    opened_at,
    prior_rows_3d,
    prior_csat_3d,
    prior_rows_3d_percent_rank,
    agent_id_min_prior_rows_3d
FROM sequenced
WHERE opened_at >= TIMESTAMP '2025-01-25 11:00:00'
ORDER BY prior_rows_3d_percent_rank, prior_rows_3d
