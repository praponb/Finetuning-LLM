WITH in_window AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.agent_id,
        a.opened_at,
        COUNT(b.id) AS prior_rows_1d,
        SUM(b.csat) AS prior_csat_1d
    FROM tickets a
    LEFT JOIN tickets b
      ON b.agent_id = a.agent_id
     AND b.opened_at < a.opened_at
     AND b.opened_at >= a.opened_at - INTERVAL 1 DAYS
    GROUP BY a.id, a.agent_id, a.opened_at
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTH_VALUE(prior_csat_1d, 2) OVER (PARTITION BY agent_id, prior_rows_1d ORDER BY prior_csat_1d ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_prior_csat_1d,
        ROUND(prior_csat_1d / NULLIF(SUM(prior_csat_1d) OVER (PARTITION BY prior_rows_1d), 0), 4) AS prior_csat_1d_share_of_prior_rows_1d
    FROM in_window
)
-- Final projection
SELECT
    id,
    agent_id,
    prior_rows_1d,
    prior_csat_1d,
    prior_csat_1d_share_of_prior_rows_1d
FROM with_windows
ORDER BY prior_rows_1d DESC
