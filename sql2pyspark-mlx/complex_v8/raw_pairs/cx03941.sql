WITH cleaned AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        csat,
        tags,
        DATE_TRUNC('WEEK', opened_at) AS opened_at_week
    FROM tickets
    WHERE TO_DATE(closed_at) < DATE '2025-03-04'
      AND csat >= 2
),
joined_agent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.tags,
        su.agent_name,
        su.team AS agent_team
    FROM cleaned s
    JOIN support_agents su ON su.id = s.agent_id
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        MIN(requester_id) OVER w_agent_team AS agent_team_min_requester_id,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY agent_team)) AS distinct_agent_id_in_agent_team,
        MIN(requester_id) OVER (PARTITION BY agent_team ORDER BY requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_requester_id_so_far,
        LAG(requester_id) OVER (ORDER BY requester_id DESC, id) AS prev_requester_id,
        DENSE_RANK() OVER (PARTITION BY agent_team ORDER BY requester_id DESC, id) AS requester_id_dense_rank,
        SUM(requester_id) OVER w_agent_team AS agent_team_sum_requester_id,
        COUNT(*) OVER (PARTITION BY agent_team) AS rows_in_agent_team,
        ROUND(requester_id - AVG(requester_id) OVER (PARTITION BY agent_team), 2) AS requester_id_vs_agent_team_avg,
        SUM(requester_id) OVER (PARTITION BY agent_team ORDER BY requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_requester_id
    FROM joined_agent
    WINDOW w_agent_team AS (PARTITION BY agent_team)
),
top_per_group AS (
    -- First row per group
    SELECT *
    FROM with_windows
    WHERE requester_id_dense_rank = 1
),
with_windows_2 AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        requester_id,
        agent_team,
        min_requester_id_so_far,
        prev_requester_id,
        requester_id_dense_rank,
        agent_team_sum_requester_id,
        requester_id_vs_agent_team_avg,
        running_requester_id,
        ROUND(PERCENT_RANK() OVER (PARTITION BY agent_id ORDER BY requester_id_dense_rank DESC), 3) AS requester_id_dense_rank_percent_rank,
        ROW_NUMBER() OVER w_agent_id_by_requester_id_den_2 AS requester_id_dense_rank_seq,
        COUNT(*) OVER (PARTITION BY agent_team, requester_id_dense_rank) AS rows_in_agent_team_requester_id_dense_rank,
        requester_id_dense_rank - LAG(requester_id_dense_rank) OVER w_requester_id_dense_rank_by_r AS requester_id_dense_rank_change,
        DENSE_RANK() OVER w_agent_id_by_requester_id_den AS requester_id_dense_rank_dense_rank
    FROM top_per_group
    WINDOW w_agent_id_by_requester_id_den_2 AS (PARTITION BY agent_id ORDER BY requester_id_dense_rank DESC, id), w_requester_id_dense_rank_by_r AS (PARTITION BY requester_id_dense_rank ORDER BY requester_id, id), w_agent_id_by_requester_id_den AS (PARTITION BY agent_id ORDER BY requester_id_dense_rank DESC)
),
stats AS (
    -- Group-level totals
    SELECT
        requester_id_dense_rank,
        requester_id_dense_rank_seq,
        agent_team,
        SUM(rows_in_agent_team_requester_id_dense_rank) AS total_rows_in_agent_team_requester_id_dense_rank,
        COUNT(*) FILTER (WHERE agent_team IN ('Billing', 'Onboarding')) AS n_agent_team_in_billing_onboar,
        ARRAY_SORT(COLLECT_SET(agent_team)) AS agent_team_list,
        SUM(rows_in_agent_team_requester_id_dense_rank) FILTER (WHERE agent_team IN ('Tech', 'Billing', 'Onboarding')) AS rows_in_agent_team_requester_id_dense_rank_agent,
        MAX(min_requester_id_so_far) AS max_min_requester_id_so_far,
        MIN(requester_id_dense_rank_seq) AS min_requester_id_dense_rank_seq
    FROM with_windows_2
    GROUP BY requester_id_dense_rank, requester_id_dense_rank_seq, agent_team
)
SELECT
    requester_id_dense_rank,
    requester_id_dense_rank_seq,
    agent_team,
    total_rows_in_agent_team_requester_id_dense_rank,
    n_agent_team_in_billing_onboar,
    agent_team_list,
    rows_in_agent_team_requester_id_dense_rank_agent,
    max_min_requester_id_so_far,
    min_requester_id_dense_rank_seq,
    CASE WHEN agent_team IN ('VIP', 'Billing') THEN 'group_a' WHEN agent_team = 'Onboarding' THEN 'group_b' ELSE 'other' END AS agent_team_group
FROM stats
ORDER BY rows_in_agent_team_requester_id_dense_rank_agent DESC, requester_id_dense_rank_seq, requester_id_dense_rank, agent_team
LIMIT 20
