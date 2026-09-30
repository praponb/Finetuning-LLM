WITH covered AS (
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
agent_id_stats AS (
    -- Avg prior_rows_3d per agent_id
    SELECT agent_id, AVG(prior_rows_3d) AS avg_prior_rows_3d
    FROM covered
    GROUP BY agent_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_prior_rows_3d,
        ROUND(s.prior_rows_3d - g.avg_prior_rows_3d, 2) AS prior_rows_3d_minus_avg
    FROM covered s
    JOIN agent_id_stats g ON g.agent_id = s.agent_id
    WHERE s.prior_rows_3d > g.avg_prior_rows_3d
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        agent_id,
        opened_at,
        avg_prior_rows_3d,
        prior_rows_3d_minus_avg,
        prior_rows_3d - LAG(prior_rows_3d) OVER (PARTITION BY prior_rows_3d ORDER BY prior_rows_3d DESC NULLS FIRST, id) AS prior_rows_3d_change,
        prior_rows_3d_minus_avg - LAG(prior_rows_3d_minus_avg) OVER w_prior_rows_3d_by_prior_rows AS prior_rows_3d_minus_avg_change,
        SUM(avg_prior_rows_3d) OVER (PARTITION BY prior_rows_3d ORDER BY prior_rows_3d DESC NULLS FIRST, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS avg_prior_rows_3d_sum_3_prior
    FROM outliers
    WINDOW w_prior_rows_3d_by_prior_rows AS (PARTITION BY prior_rows_3d ORDER BY prior_rows_3d DESC NULLS FIRST, id)
),
totals AS (
    -- Group-level totals
    SELECT
        CASE WHEN prior_rows_3d_minus_avg >= 4.0 THEN 'A' WHEN prior_rows_3d_minus_avg >= -3.2 THEN 'B' ELSE 'C' END AS prior_rows_3d_minus_avg_tier,
        DATE_TRUNC('MONTH', opened_at) AS opened_at_month,
        ROUND(SUM(prior_rows_3d_minus_avg_change), 2) AS total_prior_rows_3d_minus_avg_change,
        MAX(opened_at) AS last_opened_at,
        MIN(opened_at) AS first_opened_at
    FROM with_windows
    GROUP BY CASE WHEN prior_rows_3d_minus_avg >= 4.0 THEN 'A' WHEN prior_rows_3d_minus_avg >= -3.2 THEN 'B' ELSE 'C' END, DATE_TRUNC('MONTH', opened_at)
    HAVING SUM(prior_rows_3d_change) > -5
)
-- Final projection
SELECT
    prior_rows_3d_minus_avg_tier,
    opened_at_month,
    total_prior_rows_3d_minus_avg_change,
    last_opened_at
FROM totals
ORDER BY total_prior_rows_3d_minus_avg_change DESC NULLS FIRST
