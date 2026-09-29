WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_name,
        team,
        tier,
        hired_on,
        CONCAT_WS(' / ', agent_name, tier) AS agent_name_tier_label,
        INITCAP(TRIM(tier)) AS tier_clean,
        UPPER(TRIM(tier)) AS tier_clean_2
    FROM support_agents
    WHERE tier <> 'T2'
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        agent_name,
        team,
        tier,
        tier_clean,
        tier_clean_2,
        RANK() OVER (ORDER BY hired_on DESC, id) AS hired_on_rank,
        LAG(hired_on) OVER w_all_by_hired_on AS prev_hired_on,
        NTH_VALUE(hired_on, 2) OVER (PARTITION BY tier, team ORDER BY hired_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_hired_on,
        DATEDIFF(hired_on, LAG(hired_on) OVER (PARTITION BY tier, team ORDER BY hired_on DESC, id)) AS days_since_prev_hired_on,
        ROUND(PERCENT_RANK() OVER w_tier_team_by_hired_on, 3) AS hired_on_percent_rank
    FROM base
    WINDOW w_all_by_hired_on AS (ORDER BY hired_on DESC, id), w_tier_team_by_hired_on AS (PARTITION BY tier, team ORDER BY hired_on DESC, id)
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        COALESCE(hired_on_percent_rank, 0.0) AS hired_on_percent_rank_filled,
        MONTH(second_hired_on) AS second_hired_on_month
    FROM sequenced
    WHERE UPPER(TRIM(tier_clean)) LIKE 'T%'
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        agent_name,
        tier_clean,
        hired_on_rank,
        prev_hired_on,
        days_since_prev_hired_on,
        hired_on_percent_rank_filled,
        second_hired_on_month,
        MAX(hired_on_percent_rank_filled) OVER (PARTITION BY team, hired_on_rank) AS team_hired_on_rank_max_hired_on_percent_rank_fil,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', prev_hired_on) ORDER BY prev_hired_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        SIZE(COLLECT_SET(tier) OVER (PARTITION BY team, hired_on_rank)) AS distinct_tier_in_team_hired_on_rank,
        COUNT(*) OVER (PARTITION BY team, hired_on_rank) AS rows_in_team_hired_on_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY hired_on_rank, second_hired_on_month ORDER BY prev_hired_on ASC, id), 3) AS prev_hired_on_cume_dist
    FROM labeled
)
-- Final projection
SELECT
    id,
    agent_name,
    hired_on_rank,
    days_since_prev_hired_on,
    hired_on_percent_rank_filled,
    second_hired_on_month,
    team_hired_on_rank_max_hired_on_percent_rank_fil,
    running_count,
    distinct_tier_in_team_hired_on_rank,
    rows_in_team_hired_on_rank,
    prev_hired_on_cume_dist,
    SUBSTRING(TRIM(tier_clean), 1, 2) AS tier_clean_prefix,
    LAST_DAY(prev_hired_on) AS prev_hired_on_month_end
FROM with_windows
WHERE rows_in_team_hired_on_rank > 3
ORDER BY distinct_tier_in_team_hired_on_rank, prev_hired_on_cume_dist
