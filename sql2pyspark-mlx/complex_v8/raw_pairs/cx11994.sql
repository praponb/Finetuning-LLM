WITH matched AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.agent_id,
        a.opened_at,
        COUNT(b.id) AS prior_rows_3d,
        SUM(b.requester_id) AS prior_requester_id_3d
    FROM tickets a
    LEFT JOIN tickets b
      ON b.agent_id = a.agent_id
     AND b.opened_at < a.opened_at
     AND b.opened_at >= a.opened_at - INTERVAL 3 DAYS
    GROUP BY a.id, a.agent_id, a.opened_at
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER w_prior_rows_3d_agent_id AS rows_in_prior_rows_3d_agent_id,
        FIRST_VALUE(opened_at) OVER (PARTITION BY MONTH(opened_at) ORDER BY opened_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_opened_at,
        ROUND(AVG(prior_rows_3d) OVER (PARTITION BY MONTH(opened_at) ORDER BY opened_at NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS prior_rows_3d_avg_2
    FROM matched
    WINDOW w_prior_rows_3d_agent_id AS (PARTITION BY prior_rows_3d, agent_id)
),
agent_id_stats AS (
    -- Median prior_requester_id_3d per agent_id
    SELECT agent_id, PERCENTILE_APPROX(prior_requester_id_3d, 0.5) AS median_prior_requester_id_3d
    FROM sequenced
    GROUP BY agent_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_prior_requester_id_3d,
        ROUND(s.prior_requester_id_3d - g.median_prior_requester_id_3d, 2) AS prior_requester_id_3d_minus_median
    FROM sequenced s
    JOIN agent_id_stats g ON g.agent_id = s.agent_id
    WHERE s.prior_requester_id_3d >= g.median_prior_requester_id_3d
)
SELECT
    id,
    agent_id,
    opened_at,
    prior_rows_3d,
    prior_requester_id_3d,
    rows_in_prior_rows_3d_agent_id,
    first_opened_at,
    prior_rows_3d_avg_2,
    prior_requester_id_3d_minus_median,
    LEAST(prior_requester_id_3d, prior_rows_3d) AS least_prior_requester_id_3d_prior_rows_3d,
    ROUND(prior_requester_id_3d / NULLIF(prior_rows_3d_avg_2, 0), 3) AS prior_requester_id_3d_per_prior_rows_3d_avg_2
FROM compared
ORDER BY first_opened_at NULLS LAST
