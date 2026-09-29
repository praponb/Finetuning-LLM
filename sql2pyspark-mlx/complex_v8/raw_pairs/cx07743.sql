WITH matched AS (
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
with_details AS (
    -- Attach agent attributes
    SELECT
        s.*,
        su.tier AS agent_tier,
        su.agent_name,
        su.team AS agent_team
    FROM matched s
    JOIN support_agents su ON su.id = s.agent_id
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        RANK() OVER (PARTITION BY TO_DATE(opened_at), prior_rows_12h ORDER BY prior_csat_12h DESC) AS prior_csat_12h_rank,
        ROUND(prior_csat_12h - AVG(prior_csat_12h) OVER (PARTITION BY agent_id, agent_tier), 2) AS prior_csat_12h_vs_agent_id_agent_tier_avg,
        LAG(prior_csat_12h) OVER (ORDER BY prior_rows_12h DESC, id) AS prev_prior_csat_12h
    FROM with_details
),
profiled AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT MAX(t.closed_at) FROM tickets t WHERE t.agent_id = s.agent_id) AS last_ticket_closed_at,
        (SELECT MAX(t.closed_at) FROM tickets t WHERE t.agent_id = s.agent_id) AS last_ticket_closed_at_2
    FROM windowed s
)
-- Subtotals with GROUPING SETS
SELECT
    agent_team,
    agent_id,
    GROUPING_ID() AS grouping_level,
    COUNT(*) AS cnt,
    ROUND(STDDEV(prior_rows_12h), 2) AS stddev_prior_rows_12h
FROM profiled
GROUP BY GROUPING SETS ((agent_team, agent_id), (agent_team), (agent_id))
ORDER BY agent_team NULLS LAST, agent_id NULLS LAST
