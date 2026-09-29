WITH filtered AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        DATEDIFF(DATE '2025-06-30', opened_at) AS days_since_opened_at,
        CASE WHEN priority IN ('P1', 'P2') THEN 'group_a' WHEN priority = 'P4' THEN 'group_b' ELSE 'other' END AS priority_group
    FROM tickets
    WHERE (requester_id > 8 OR priority = 'P2')
),
with_details_agent AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.priority,
        s.channel,
        s.csat,
        s.days_since_opened_at,
        s.priority_group,
        su.agent_name,
        su.team AS agent_team
    FROM filtered s
    JOIN support_agents su ON su.id = s.agent_id
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        closed_at,
        csat,
        days_since_opened_at,
        priority_group,
        agent_name,
        agent_team,
        ROUND(AVG(days_since_opened_at) OVER (PARTITION BY agent_team), 2) AS agent_team_avg_days_since_opened_at,
        SUM(csat) OVER (PARTITION BY agent_team) AS agent_team_sum_csat,
        LEAD(days_since_opened_at) OVER (PARTITION BY channel, agent_id ORDER BY closed_at DESC, id) AS next_days_since_opened_at,
        ROUND(days_since_opened_at / NULLIF(SUM(days_since_opened_at) OVER (PARTITION BY agent_team), 0), 4) AS days_since_opened_at_share_of_agent_team,
        ROW_NUMBER() OVER (PARTITION BY channel, agent_id ORDER BY closed_at DESC, id) AS seq_no,
        COUNT(*) OVER (PARTITION BY channel, agent_id ORDER BY closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(days_since_opened_at - AVG(days_since_opened_at) OVER (PARTITION BY agent_team), 2) AS days_since_opened_at_vs_agent_team_avg,
        SIZE(COLLECT_SET(priority_group) OVER w_agent_team) AS distinct_priority_group_in_agent_team,
        NTH_VALUE(days_since_opened_at, 2) OVER (PARTITION BY channel, agent_id ORDER BY closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_days_since_opened_at,
        SIZE(COLLECT_SET(channel) OVER w_agent_team) AS distinct_channel_in_agent_team
    FROM with_details_agent
    WINDOW w_agent_team AS (PARTITION BY agent_team)
),
with_trends_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY csat, priority_group) AS rows_in_csat_priority_group,
        FIRST_VALUE(seq_no) IGNORE NULLS OVER (ORDER BY second_days_since_opened_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_seq_no,
        NTILE(3) OVER w_all_by_second_days_since_ope_2 AS second_days_since_opened_at_tercile,
        NTILE(10) OVER w_all_by_second_days_since_ope_2 AS second_days_since_opened_at_decile
    FROM with_trends
    WINDOW w_all_by_second_days_since_ope_2 AS (ORDER BY second_days_since_opened_at DESC, id)
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        second_days_since_opened_at_tercile,
        SUM(days_since_opened_at) AS total_days_since_opened_at,
        ROUND(SUM(days_since_opened_at_share_of_agent_team) / NULLIF(SUM(days_since_opened_at), 0), 3) AS days_since_opened_at_share_of_agent_team_to_days,
        ROUND(SUM(days_since_opened_at_share_of_agent_team) / NULLIF(SUM(csat), 0), 3) AS days_since_opened_at_share_of_agent_team_to_csat,
        MIN(closed_at) AS first_closed_at
    FROM with_trends_2
    GROUP BY 1
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(CUME_DIST() OVER w_second_days_since_opened_at_2, 3) AS first_closed_at_cume_dist,
        MIN(total_days_since_opened_at) OVER (PARTITION BY second_days_since_opened_at_tercile ORDER BY first_closed_at DESC NULLS FIRST, second_days_since_opened_at_tercile ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_total_days_since_opened_at_so_far,
        ROUND(total_days_since_opened_at / NULLIF(SUM(total_days_since_opened_at) OVER (PARTITION BY second_days_since_opened_at_tercile), 0), 4) AS total_days_since_opened_at_share_of_second_days_,
        NTILE(4) OVER w_second_days_since_opened_at_4 AS first_closed_at_quartile
    FROM grouped
    WINDOW w_second_days_since_opened_at_2 AS (PARTITION BY second_days_since_opened_at_tercile ORDER BY first_closed_at DESC NULLS FIRST), w_second_days_since_opened_at_4 AS (PARTITION BY second_days_since_opened_at_tercile ORDER BY first_closed_at DESC NULLS FIRST, second_days_since_opened_at_tercile)
)
-- Final projection
SELECT
    second_days_since_opened_at_tercile,
    days_since_opened_at_share_of_agent_team_to_days,
    first_closed_at,
    min_total_days_since_opened_at_so_far,
    total_days_since_opened_at_share_of_second_days_,
    first_closed_at_quartile,
    ABS(first_closed_at_quartile - 5) AS first_closed_at_quartile_distance,
    CASE WHEN first_closed_at_quartile >= 7 THEN 'HIGH' WHEN first_closed_at_quartile >= 4 THEN 'MEDIUM' ELSE 'LOW' END AS first_closed_at_quartile_tier
FROM windowed
