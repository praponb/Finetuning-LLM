WITH matched AS (
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
annotated AS (
    -- Label and score rows
    SELECT
        id,
        agent_id,
        prior_csat_3d,
        DATE_TRUNC('YEAR', opened_at) AS opened_at_year,
        CASE WHEN prior_rows_3d >= 6 THEN 'HIGH' WHEN prior_rows_3d >= 3 THEN 'MEDIUM' ELSE 'LOW' END AS prior_rows_3d_tier
    FROM matched
),
agent_id_stats AS (
    -- Avg prior_csat_3d per agent_id
    SELECT agent_id, AVG(prior_csat_3d) AS avg_prior_csat_3d
    FROM annotated
    GROUP BY agent_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_prior_csat_3d,
        ROUND(s.prior_csat_3d - g.avg_prior_csat_3d, 2) AS prior_csat_3d_minus_avg
    FROM annotated s
    JOIN agent_id_stats g ON g.agent_id = s.agent_id
    WHERE s.prior_csat_3d > g.avg_prior_csat_3d
)
-- Final projection
SELECT
    id,
    agent_id,
    prior_csat_3d,
    opened_at_year,
    prior_rows_3d_tier,
    prior_csat_3d_minus_avg
FROM vs_group
ORDER BY prior_rows_3d_tier DESC, id
LIMIT 5
