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
agent_id_stats AS (
    -- Avg prior_csat_3d per agent_id
    SELECT agent_id, AVG(prior_csat_3d) AS avg_prior_csat_3d
    FROM in_window
    GROUP BY agent_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_prior_csat_3d,
        ROUND(s.prior_csat_3d - g.avg_prior_csat_3d, 2) AS prior_csat_3d_minus_avg
    FROM in_window s
    JOIN agent_id_stats g ON g.agent_id = s.agent_id
    WHERE s.prior_csat_3d > g.avg_prior_csat_3d
),
summarized_rows AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN t.priority <> 'P3' THEN 1 END) FROM tickets t WHERE t.agent_id = s.agent_id) AS ticket_priority_count,
        (SELECT COUNT(*) FROM tickets t WHERE t.agent_id = s.agent_id) AS ticket_count
    FROM vs_group s
)
SELECT
    id,
    opened_at,
    prior_rows_3d,
    prior_csat_3d,
    avg_prior_csat_3d,
    ticket_priority_count,
    ticket_count
FROM summarized_rows
ORDER BY opened_at DESC NULLS FIRST, id
LIMIT 5
