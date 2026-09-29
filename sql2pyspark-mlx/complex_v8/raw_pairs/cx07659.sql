WITH in_window AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.agent_id,
        a.opened_at,
        COUNT(b.id) AS prior_rows_12h,
        SUM(b.csat) AS prior_csat_12h
    FROM tickets a
    LEFT JOIN tickets b
      ON b.agent_id = a.agent_id
     AND b.opened_at < a.opened_at
     AND b.opened_at >= a.opened_at - INTERVAL 12 HOURS
    GROUP BY a.id, a.agent_id, a.opened_at
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(prior_csat_12h) OVER (PARTITION BY agent_id) AS agent_id_min_prior_csat_12h,
        FIRST_VALUE(prior_rows_12h) OVER (PARTITION BY DATE_TRUNC('MONTH', opened_at), agent_id ORDER BY prior_csat_12h, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_prior_rows_12h,
        SUM(prior_csat_12h) OVER (PARTITION BY agent_id) AS agent_id_sum_prior_csat_12h,
        COUNT(*) OVER (PARTITION BY agent_id) AS rows_in_agent_id,
        DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', opened_at), agent_id ORDER BY prior_csat_12h, id) AS prior_csat_12h_dense_rank,
        NTILE(5) OVER (PARTITION BY DATE_TRUNC('MONTH', opened_at), agent_id ORDER BY prior_csat_12h, id) AS prior_csat_12h_quintile
    FROM in_window
),
with_trends_2 AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(agent_id_min_prior_csat_12h / NULLIF(SUM(agent_id_min_prior_csat_12h) OVER (PARTITION BY prior_rows_12h, prior_csat_12h_quintile), 0), 4) AS agent_id_min_prior_csat_12h_share_of_prior_rows_,
        SUM(first_prior_rows_12h) OVER (PARTITION BY prior_csat_12h_quintile, agent_id ORDER BY prior_csat_12h NULLS LAST, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS first_prior_rows_12h_sum_7,
        SUM(prior_csat_12h) OVER (PARTITION BY prior_csat_12h_quintile, agent_id ORDER BY prior_csat_12h NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS prior_csat_12h_sum_3_prior,
        ROUND(prior_csat_12h / NULLIF(SUM(prior_csat_12h) OVER (PARTITION BY prior_rows_12h, prior_csat_12h_quintile), 0), 4) AS prior_csat_12h_share_of_prior_rows_12h_prior_csa,
        ROUND(AVG(prior_csat_12h_dense_rank) OVER (PARTITION BY prior_rows_12h, prior_csat_12h_quintile), 2) AS prior_rows_12h_prior_csat_12h_quintile_avg_prior
    FROM with_trends
)
SELECT
    id,
    agent_id,
    prior_rows_12h,
    prior_csat_12h,
    agent_id_min_prior_csat_12h,
    first_prior_rows_12h,
    agent_id_sum_prior_csat_12h,
    rows_in_agent_id,
    prior_csat_12h_dense_rank,
    agent_id_min_prior_csat_12h_share_of_prior_rows_,
    prior_csat_12h_sum_3_prior
FROM with_trends_2
WHERE first_prior_rows_12h <= 7
ORDER BY agent_id_sum_prior_csat_12h NULLS LAST, prior_csat_12h_dense_rank DESC
