WITH in_window AS (
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
agent_id_stats AS (
    -- Max prior_rows_7d per agent_id
    SELECT agent_id, MAX(prior_rows_7d) AS max_prior_rows_7d
    FROM in_window
    GROUP BY agent_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_prior_rows_7d,
        ROUND(s.prior_rows_7d - g.max_prior_rows_7d, 2) AS prior_rows_7d_minus_max
    FROM in_window s
    JOIN agent_id_stats g ON g.agent_id = s.agent_id
    WHERE s.prior_rows_7d = g.max_prior_rows_7d
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        opened_at,
        prior_rows_7d,
        max_prior_rows_7d,
        prior_rows_7d_minus_max,
        DENSE_RANK() OVER (PARTITION BY agent_id, prior_rows_7d ORDER BY prior_rows_7d_minus_max ASC) AS prior_rows_7d_minus_max_dense_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY TO_DATE(opened_at), agent_id ORDER BY prior_requester_id_7d ASC, id), 3) AS prior_requester_id_7d_cume_dist,
        SUM(prior_requester_id_7d) OVER (PARTITION BY agent_id, prior_rows_7d ORDER BY prior_rows_7d_minus_max ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_prior_requester_id_7d,
        COUNT(*) OVER (PARTITION BY agent_id, prior_rows_7d) AS rows_in_agent_id_prior_rows_7d
    FROM above_average
)
SELECT id, metric, amount
FROM (
    SELECT id, prior_rows_7d, rows_in_agent_id_prior_rows_7d, running_prior_requester_id_7d, prior_rows_7d_minus_max_dense_rank
    FROM with_windows
) src
UNPIVOT INCLUDE NULLS (
    amount FOR metric IN (prior_rows_7d, rows_in_agent_id_prior_rows_7d, running_prior_requester_id_7d, prior_rows_7d_minus_max_dense_rank)
) unp
ORDER BY id, metric
