WITH matched AS (
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
totals AS (
    -- Summarise per group
    SELECT
        agent_id,
        MAX(opened_at) AS last_opened_at,
        MAX(prior_csat_7d) AS max_prior_csat_7d,
        ROUND(SUM(prior_csat_7d) / NULLIF(SUM(prior_rows_7d), 0), 3) AS prior_csat_7d_to_prior_rows_7d_ratio
    FROM matched
    GROUP BY agent_id
)
-- Final projection
SELECT agent_id, last_opened_at, max_prior_csat_7d
FROM totals
ORDER BY agent_id DESC, max_prior_csat_7d NULLS LAST
