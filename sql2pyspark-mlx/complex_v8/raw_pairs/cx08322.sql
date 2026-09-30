WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        csat,
        DATEDIFF(DATE '2025-06-30', opened_at) AS days_since_opened_at,
        CASE WHEN priority IN ('P2', 'P4') THEN 'group_a' WHEN priority = 'P3' THEN 'group_b' ELSE 'other' END AS priority_group
    FROM tickets
    WHERE (TO_DATE(closed_at) < DATE '2025-03-09' OR csat >= 2)
),
combined_details AS (
    -- Join support_agents
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.closed_at,
        s.days_since_opened_at,
        s.priority_group,
        su.tier AS agent_tier,
        su.team AS agent_team,
        su.hired_on AS agent_hired_on
    FROM prepared s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE su.team = 'VIP'
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTH_VALUE(closed_at, 2) OVER (ORDER BY days_since_opened_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_closed_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY priority_group, agent_team ORDER BY days_since_opened_at ASC), 3) AS days_since_opened_at_percent_rank,
        MIN(requester_id) OVER (PARTITION BY agent_team, agent_id) AS agent_team_agent_id_min_requester_id,
        requester_id - LAG(requester_id) OVER (PARTITION BY agent_tier, agent_id ORDER BY requester_id ASC, id) AS requester_id_change,
        COUNT(*) OVER (PARTITION BY YEAR(closed_at), agent_tier) AS rows_in_closed_at_year_agent_tier,
        NTH_VALUE(days_since_opened_at, 2) OVER (PARTITION BY priority_group, agent_team ORDER BY days_since_opened_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_days_since_opened_at,
        LEAD(days_since_opened_at) OVER w_agent_tier_closed_at_month_b AS next_days_since_opened_at,
        MAX(days_since_opened_at) OVER (PARTITION BY agent_id) AS agent_id_max_days_since_opened_at,
        ROUND(requester_id / NULLIF(SUM(requester_id) OVER (PARTITION BY agent_team, agent_id), 0), 4) AS requester_id_share_of_agent_team_agent_id,
        ROUND(AVG(days_since_opened_at) OVER (PARTITION BY agent_tier, agent_id ORDER BY requester_id ASC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS days_since_opened_at_avg_4,
        ROUND(AVG(days_since_opened_at) OVER w_closed_at_year_agent_tier, 2) AS closed_at_year_agent_tier_avg_days_since_opened_,
        ROUND(requester_id - AVG(requester_id) OVER w_closed_at_year_agent_tier, 2) AS requester_id_vs_closed_at_year_agent_tier_avg
    FROM combined_details
    WINDOW w_agent_tier_closed_at_month_b AS (PARTITION BY agent_tier, MONTH(closed_at) ORDER BY closed_at, id), w_closed_at_year_agent_tier AS (PARTITION BY YEAR(closed_at), agent_tier)
),
labeled AS (
    -- Add calculated columns
    SELECT *, INITCAP(TRIM(priority_group)) AS priority_group_clean
    FROM windowed
    WHERE requester_id_share_of_agent_team_agent_id BETWEEN 0.3 AND 0.9
),
per_group AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', second_closed_at) AS second_closed_at_month,
        ROUND(SUM(days_since_opened_at_avg_4), 2) AS total_days_since_opened_at_avg_4,
        SUM(agent_id_max_days_since_opened_at) AS total_agent_id_max_days_since_opened_at,
        COUNT(*) AS n_rows,
        SUM(second_days_since_opened_at) AS total_second_days_since_opened_at
    FROM labeled
    WHERE second_closed_at < TIMESTAMP '2025-02-24 13:00:00'
    GROUP BY DATE_TRUNC('MONTH', second_closed_at)
    HAVING AVG(requester_id_share_of_agent_team_agent_id) > 0.1
),
windowed_2 AS (
    -- Previous / next values per key
    SELECT
        second_closed_at_month,
        total_days_since_opened_at_avg_4,
        n_rows,
        total_second_days_since_opened_at,
        ROUND(n_rows / NULLIF(SUM(n_rows) OVER (PARTITION BY n_rows), 0), 4) AS n_rows_share_of_n_rows,
        MAX(n_rows) OVER (PARTITION BY n_rows ORDER BY total_agent_id_max_days_since_opened_at ASC, second_closed_at_month ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS n_rows_max_3_prior,
        ROUND(CUME_DIST() OVER (PARTITION BY n_rows ORDER BY total_agent_id_max_days_since_opened_at ASC, second_closed_at_month), 3) AS total_agent_id_max_days_since_opened_at_cume_dis
    FROM per_group
)
-- Final projection
SELECT
    second_closed_at_month,
    n_rows,
    total_second_days_since_opened_at,
    n_rows_share_of_n_rows,
    total_agent_id_max_days_since_opened_at_cume_dis,
    DATEDIFF(DATE '2026-01-01', second_closed_at_month) AS days_since_second_closed_at_month,
    FLOOR(n_rows_max_3_prior / 5) * 5 AS n_rows_max_3_prior_bucket
FROM windowed_2
ORDER BY n_rows_max_3_prior_bucket NULLS LAST, days_since_second_closed_at_month DESC
