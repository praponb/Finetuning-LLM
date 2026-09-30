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
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(prior_requester_id_7d) OVER (PARTITION BY MONTH(opened_at), prior_rows_7d ORDER BY prior_rows_7d, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS prior_requester_id_7d_avg_7,
        prior_requester_id_7d - LAG(prior_requester_id_7d) OVER (PARTITION BY MONTH(opened_at), prior_rows_7d ORDER BY prior_rows_7d, id) AS prior_requester_id_7d_change,
        MAX(prior_rows_7d) OVER (PARTITION BY agent_id) AS agent_id_max_prior_rows_7d,
        SUM(prior_rows_7d) OVER (PARTITION BY prior_rows_7d, agent_id ORDER BY prior_rows_7d ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_prior_rows_7d,
        LAG(prior_requester_id_7d, 2) OVER (PARTITION BY prior_rows_7d, agent_id ORDER BY prior_rows_7d ASC, id) AS prev2_prior_requester_id_7d,
        LAG(opened_at, 3) OVER (PARTITION BY MONTH(opened_at), prior_rows_7d ORDER BY prior_rows_7d, id) AS prev3_opened_at
    FROM in_window
),
with_activity AS (
    -- Keep rows whose agent matches
    SELECT s.*
    FROM ranked s
    WHERE EXISTS (
            SELECT 1
            FROM support_agents su
            WHERE su.id = s.agent_id
              AND LENGTH(TRIM(su.agent_name)) >= 7
        )
),
summarized_rows AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT ROUND(AVG(t.requester_id), 2) FROM tickets t WHERE t.agent_id = s.agent_id) AS avg_ticket_requester_id
    FROM with_activity s
)
SELECT
    agent_id,
    prior_rows_7d,
    GROUPING(agent_id) AS is_total_agent_id,
    MIN(prior_requester_id_7d_avg_7) AS min_prior_requester_id_7d_avg_7
FROM summarized_rows
GROUP BY GROUPING SETS ((agent_id, prior_rows_7d), (agent_id), (prior_rows_7d))
ORDER BY agent_id NULLS LAST, prior_rows_7d NULLS LAST
