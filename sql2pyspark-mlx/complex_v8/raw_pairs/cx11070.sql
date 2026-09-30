WITH joined_ranges AS (
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
joined_agent AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.prior_rows_7d,
        s.prior_csat_7d,
        su.tier AS agent_tier
    FROM joined_ranges s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_FORMAT(opened_at, 'yyyy-MM') AS opened_at_ym,
        COUNT(DISTINCT id) AS distinct_ids,
        ROUND(STDDEV(prior_rows_7d), 2) AS stddev_prior_rows_7d,
        COUNT(*) FILTER (WHERE agent_tier = 'T2') AS n_agent_tier_t2
    FROM joined_agent
    GROUP BY 1
)
SELECT
    opened_at_ym,
    distinct_ids,
    stddev_prior_rows_7d,
    n_agent_tier_t2,
    ROUND(stddev_prior_rows_7d * 1.5, 2) AS stddev_prior_rows_7d_adj,
    FLOOR(distinct_ids / 2) * 2 AS distinct_ids_bucket
FROM grouped
ORDER BY n_agent_tier_t2
