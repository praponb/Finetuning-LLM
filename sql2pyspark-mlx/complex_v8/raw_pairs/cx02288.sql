WITH in_window AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.agent_id,
        a.opened_at,
        COUNT(b.id) AS prior_rows_7d,
        SUM(b.requester_id) AS prior_requester_id_7d
    FROM tickets a
    LEFT JOIN tickets b
      ON b.agent_id = a.agent_id
     AND b.opened_at < a.opened_at
     AND b.opened_at >= a.opened_at - INTERVAL 7 DAYS
    GROUP BY a.id, a.agent_id, a.opened_at
),
with_details AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.prior_requester_id_7d,
        su.team AS agent_team
    FROM in_window s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        agent_id,
        opened_at,
        agent_team,
        MAX(prior_requester_id_7d) OVER (ORDER BY opened_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_prior_requester_id_7d_so_far,
        NTILE(5) OVER w_all_by_opened_at AS opened_at_quintile,
        DENSE_RANK() OVER w_all_by_opened_at AS opened_at_dense_rank,
        FIRST_VALUE(prior_requester_id_7d) OVER (ORDER BY opened_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_prior_requester_id_7d
    FROM with_details
    WINDOW w_all_by_opened_at AS (ORDER BY opened_at DESC, id)
)
-- Subtotals with GROUPING SETS
SELECT
    agent_id,
    opened_at_quintile,
    GROUPING(agent_id) AS is_total_agent_id,
    ROUND(100 * COUNT(CASE WHEN agent_team <> 'Tech' THEN 1 END) / COUNT(*), 1) AS pct_agent_team_tech
FROM with_windows
GROUP BY GROUPING SETS ((agent_id, opened_at_quintile), (agent_id), (opened_at_quintile), ())
ORDER BY agent_id NULLS LAST, opened_at_quintile NULLS LAST
