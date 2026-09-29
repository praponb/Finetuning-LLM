WITH in_window AS (
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
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(prior_csat_3d - AVG(prior_csat_3d) OVER (PARTITION BY agent_id, MONTH(opened_at)), 2) AS prior_csat_3d_vs_agent_id_opened_at_month_avg,
        ROUND(AVG(prior_rows_3d) OVER (ORDER BY prior_rows_3d, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS prior_rows_3d_avg_3,
        DENSE_RANK() OVER (ORDER BY prior_rows_3d) AS prior_rows_3d_dense_rank,
        LAG(prior_csat_3d, 1, 0) OVER w_all_by_prior_rows_3d AS prev_prior_csat_3d,
        ROUND(prior_rows_3d - AVG(prior_rows_3d) OVER (PARTITION BY agent_id, MONTH(opened_at)), 2) AS prior_rows_3d_vs_agent_id_opened_at_month_avg,
        ROUND(prior_rows_3d / NULLIF(SUM(prior_rows_3d) OVER (PARTITION BY agent_id, MONTH(opened_at)), 0), 4) AS prior_rows_3d_share_of_agent_id_opened_at_month
    FROM in_window
    WINDOW w_all_by_prior_rows_3d AS (ORDER BY prior_rows_3d, id)
)
SELECT
    id,
    agent_id,
    opened_at,
    prior_csat_3d,
    prior_csat_3d_vs_agent_id_opened_at_month_avg,
    prior_rows_3d_avg_3,
    prior_rows_3d_dense_rank,
    prev_prior_csat_3d,
    prior_rows_3d_vs_agent_id_opened_at_month_avg,
    prior_rows_3d_share_of_agent_id_opened_at_month
FROM with_trends
WHERE prior_rows_3d_vs_agent_id_opened_at_month_avg <= 1.6
ORDER BY prior_csat_3d_vs_agent_id_opened_at_month_avg
