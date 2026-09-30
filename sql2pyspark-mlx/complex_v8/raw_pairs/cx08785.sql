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
detail_rows_events AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.avg_prior_rows_3d,
        s.prior_rows_3d_minus_avg,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_at
    FROM outliers s
    LEFT JOIN ticket_events t ON t.ticket_id = s.id AND MONTH(t.event_at) IN (1, 2)
),
with_parent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.prior_rows_3d_minus_avg,
        s.event_id,
        s.event_actor,
        su.hired_on AS agent_hired_on,
        su.tier AS agent_tier,
        su.team AS agent_team
    FROM detail_rows_events s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE su.hired_on IS NOT NULL
)
SELECT
    agent_team,
    event_id,
    COUNT(*) AS cnt,
    ROUND(SUM(prior_rows_3d_minus_avg), 2) AS total_prior_rows_3d_minus_avg
FROM with_parent
GROUP BY GROUPING SETS ((agent_team, event_id), (agent_team))
