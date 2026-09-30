WITH recent_agents AS (
    -- Scope the support_agents rows
    SELECT
        id,
        agent_name,
        team,
        tier,
        hired_on,
        DATE_FORMAT(hired_on, 'yyyy-MM-dd') AS hired_on_label,
        CASE WHEN team IN ('Billing', 'Tech') THEN 'group_a' WHEN team = 'VIP' THEN 'group_b' ELSE 'other' END AS team_group
    FROM support_agents
    WHERE LOWER(agent_name) LIKE '%wo%'
      AND team = 'Tech'
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY team_group ORDER BY hired_on, id), 3) AS hired_on_cume_dist,
        NTH_VALUE(hired_on, 2) OVER (PARTITION BY team_group, tier ORDER BY hired_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_hired_on,
        DENSE_RANK() OVER w_team_group_tier_by_hired_on_2 AS hired_on_dense_rank,
        LAST_VALUE(hired_on) OVER (ORDER BY hired_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_hired_on,
        LEAD(hired_on, 3) OVER w_team_group_by_hired_on AS next3_hired_on,
        ROW_NUMBER() OVER (PARTITION BY team_group, tier ORDER BY hired_on, id) AS seq_no,
        NTILE(10) OVER w_team_group_tier_by_hired_on_3 AS hired_on_decile,
        LAST_VALUE(hired_on) IGNORE NULLS OVER (PARTITION BY team_group, tier ORDER BY hired_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_hired_on,
        LAG(hired_on) OVER w_team_group_by_hired_on AS prev_hired_on,
        ROUND(CUME_DIST() OVER (PARTITION BY team_group, tier ORDER BY hired_on), 3) AS hired_on_cume_dist_2,
        ROUND(CUME_DIST() OVER (ORDER BY hired_on DESC, id), 3) AS hired_on_cume_dist_3,
        NTH_VALUE(hired_on, 2) OVER (PARTITION BY team_group ORDER BY hired_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_hired_on_2,
        LAG(hired_on) OVER (ORDER BY hired_on DESC, id) AS prev_hired_on_2,
        NTILE(10) OVER w_all_by_hired_on AS hired_on_decile_2
    FROM recent_agents
    WINDOW w_team_group_tier_by_hired_on_2 AS (PARTITION BY team_group, tier ORDER BY hired_on), w_team_group_by_hired_on AS (PARTITION BY team_group ORDER BY hired_on, id), w_team_group_tier_by_hired_on_3 AS (PARTITION BY team_group, tier ORDER BY hired_on, id), w_all_by_hired_on AS (ORDER BY hired_on DESC, id)
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER w_team_by_hired_on_cume_dist_t AS hired_on_cume_dist_dense_rank,
        LEAD(second_hired_on) OVER (PARTITION BY team ORDER BY seq_no DESC, id) AS next_second_hired_on,
        SUM(hired_on_decile) OVER (PARTITION BY team ORDER BY hired_on_cume_dist DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS hired_on_decile_sum_7,
        ROUND(hired_on_cume_dist_2 - AVG(hired_on_cume_dist_2) OVER w_prev_hired_on_month, 2) AS hired_on_cume_dist_2_vs_prev_hired_on_month_avg,
        SUM(hired_on_cume_dist_2) OVER (PARTITION BY DATE_TRUNC('MONTH', prev_hired_on)) AS prev_hired_on_month_sum_hired_on_cume_dist_2,
        LAG(last_hired_on, 3) OVER (PARTITION BY team ORDER BY seq_no DESC, id) AS prev3_last_hired_on,
        LAG(hired_on, 2) OVER w_team_by_hired_on_cume_dist AS prev2_hired_on
    FROM metrics
    WINDOW w_team_by_hired_on_cume_dist_t AS (PARTITION BY team ORDER BY hired_on_cume_dist DESC), w_prev_hired_on_month AS (PARTITION BY DATE_TRUNC('MONTH', prev_hired_on)), w_team_by_hired_on_cume_dist AS (PARTITION BY team ORDER BY hired_on_cume_dist DESC, id)
)
-- Final projection
SELECT
    id,
    agent_name,
    team,
    hired_on,
    hired_on_label,
    team_group,
    hired_on_cume_dist,
    second_hired_on,
    hired_on_dense_rank,
    last_hired_on,
    next3_hired_on,
    seq_no,
    hired_on_decile,
    last_known_hired_on,
    prev_hired_on,
    hired_on_cume_dist_2,
    hired_on_cume_dist_3,
    prev_hired_on_2,
    hired_on_decile_2,
    hired_on_cume_dist_dense_rank,
    next_second_hired_on,
    hired_on_decile_sum_7,
    hired_on_cume_dist_2_vs_prev_hired_on_month_avg,
    prev_hired_on_month_sum_hired_on_cume_dist_2,
    prev3_last_hired_on,
    CONCAT_WS(' / ', agent_name, hired_on_label) AS agent_name_hired_on_label_label
FROM ranked
ORDER BY last_known_hired_on DESC, agent_name_hired_on_label_label, id
LIMIT 10
