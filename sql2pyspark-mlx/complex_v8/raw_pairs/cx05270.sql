WITH filtered AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags
    FROM tickets
    WHERE channel = 'web'
),
joined_agent AS (
    -- Join support_agents
    SELECT
        s.*,
        su.agent_name,
        su.tier AS agent_tier,
        su.team AS agent_team
    FROM filtered s
    JOIN support_agents su ON su.id = s.agent_id AND su.tier IS NOT NULL
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (PARTITION BY priority, agent_team ORDER BY requester_id DESC, id) AS requester_id_rank,
        COUNT(*) OVER (PARTITION BY agent_team ORDER BY csat NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(CUME_DIST() OVER (PARTITION BY agent_tier ORDER BY closed_at DESC, id), 3) AS closed_at_cume_dist,
        SUM(requester_id) OVER (PARTITION BY channel, agent_team ORDER BY csat, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_requester_id,
        ROUND(CUME_DIST() OVER w_agent_tier_by_closed_at_ties, 3) AS closed_at_cume_dist_2,
        SIZE(COLLECT_SET(agent_team) OVER (PARTITION BY agent_id, csat)) AS distinct_agent_team_in_agent_id_csat,
        COUNT(*) OVER (PARTITION BY MONTH(opened_at), agent_team) AS rows_in_opened_at_month_agent_team,
        ROUND(csat / NULLIF(SUM(csat) OVER (PARTITION BY agent_id, csat), 0), 4) AS csat_share_of_agent_id_csat,
        NTILE(3) OVER (PARTITION BY priority, agent_team ORDER BY requester_id DESC, id) AS requester_id_tercile,
        RANK() OVER w_agent_tier_by_closed_at_ties AS closed_at_rank,
        SIZE(COLLECT_SET(agent_tier) OVER w_agent_id_csat) AS distinct_agent_tier_in_agent_id_csat,
        LAG(requester_id, 3, 0) OVER (PARTITION BY agent_tier ORDER BY closed_at DESC, id) AS prev3_requester_id,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(LAG(closed_at) OVER w_agent_tier_by_closed_at)) / 60, 1) AS minutes_since_prev_closed_at
    FROM joined_agent
    WINDOW w_agent_tier_by_closed_at_ties AS (PARTITION BY agent_tier ORDER BY closed_at DESC), w_agent_id_csat AS (PARTITION BY agent_id, csat), w_agent_tier_by_closed_at AS (PARTITION BY agent_tier ORDER BY closed_at DESC, id)
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY requester_id_tercile ORDER BY closed_at_cume_dist_2 ASC, id) AS closed_at_cume_dist_2_tercile,
        DENSE_RANK() OVER w_priority_by_running_count_ti AS running_count_dense_rank,
        ROUND(CUME_DIST() OVER (ORDER BY csat), 3) AS csat_cume_dist,
        MAX(closed_at_cume_dist_2) OVER w_priority AS priority_max_closed_at_cume_dist_2,
        RANK() OVER (PARTITION BY requester_id_tercile ORDER BY closed_at_cume_dist_2 ASC, id) AS closed_at_cume_dist_2_rank,
        LEAD(prev3_requester_id, 3) OVER (PARTITION BY priority ORDER BY running_count DESC, id) AS next3_prev3_requester_id,
        MIN(closed_at_cume_dist) OVER w_priority AS priority_min_closed_at_cume_dist
    FROM with_windows
    WINDOW w_priority_by_running_count_ti AS (PARTITION BY priority ORDER BY running_count DESC), w_priority AS (PARTITION BY priority)
),
summary AS (
    -- Summarise per group
    SELECT
        agent_team,
        DATE_TRUNC('MONTH', opened_at) AS opened_at_month,
        SUM(CASE WHEN agent_tier IN ('T1', 'T2') THEN 1 ELSE 0 END) AS n_agent_tier_in_t1_t2,
        COUNT(DISTINCT id) AS distinct_ids,
        MAX(closed_at) AS last_closed_at,
        SUM(requester_id) AS total_requester_id
    FROM metrics
    WHERE minutes_since_prev_closed_at <= 3750.0
    GROUP BY 1, 2
    HAVING AVG(requester_id_rank) > 4
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        FIRST_VALUE(total_requester_id) IGNORE NULLS OVER (PARTITION BY agent_team, n_agent_tier_in_t1_t2 ORDER BY last_closed_at DESC NULLS FIRST, agent_team, opened_at_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_total_requester_id,
        ROUND(PERCENT_RANK() OVER (PARTITION BY agent_team, n_agent_tier_in_t1_t2 ORDER BY last_closed_at DESC NULLS FIRST), 3) AS last_closed_at_percent_rank,
        ROUND(n_agent_tier_in_t1_t2 / NULLIF(SUM(n_agent_tier_in_t1_t2) OVER w_last_closed_at_to_date, 0), 4) AS n_agent_tier_in_t1_t2_share_of_last_closed_at_to,
        ROUND(CUME_DIST() OVER (PARTITION BY agent_team, n_agent_tier_in_t1_t2 ORDER BY last_closed_at DESC NULLS FIRST), 3) AS last_closed_at_cume_dist
    FROM summary
    WINDOW w_last_closed_at_to_date AS (PARTITION BY TO_DATE(last_closed_at))
)
-- Final projection
SELECT
    agent_team,
    opened_at_month,
    n_agent_tier_in_t1_t2,
    distinct_ids,
    last_closed_at,
    total_requester_id,
    last_closed_at_percent_rank,
    n_agent_tier_in_t1_t2_share_of_last_closed_at_to,
    last_closed_at_cume_dist,
    DATE_FORMAT(opened_at_month, 'yyyy-MM') AS opened_at_month_label,
    TO_DATE(opened_at_month) AS opened_at_month_date
FROM windowed
ORDER BY last_closed_at_percent_rank NULLS LAST
