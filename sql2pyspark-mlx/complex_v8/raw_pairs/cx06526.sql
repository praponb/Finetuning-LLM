WITH prepared AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel
    FROM tickets
    WHERE ARRAY_CONTAINS(tags, 'login')
      AND agent_id IS NOT NULL
),
with_parent_agent AS (
    -- Join support_agents
    SELECT s.*, su.tier AS agent_tier, su.hired_on AS agent_hired_on
    FROM prepared s
    JOIN support_agents su ON su.id = s.agent_id
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(requester_id - AVG(requester_id) OVER w_channel, 2) AS requester_id_vs_channel_avg,
        SUM(requester_id) OVER (PARTITION BY agent_tier ORDER BY agent_hired_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_requester_id,
        COUNT(*) OVER (PARTITION BY channel) AS rows_in_channel,
        FIRST_VALUE(agent_hired_on) OVER (PARTITION BY TO_DATE(opened_at) ORDER BY requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_agent_hired_on,
        DATEDIFF(agent_hired_on, LAG(agent_hired_on) OVER (PARTITION BY TO_DATE(opened_at) ORDER BY requester_id, id)) AS days_since_prev_agent_hired_on,
        NTH_VALUE(closed_at, 2) OVER (PARTITION BY channel, agent_tier ORDER BY agent_hired_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_closed_at,
        MIN(requester_id) OVER (PARTITION BY agent_tier) AS agent_tier_min_requester_id,
        ROUND((UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(LAG(opened_at) OVER (PARTITION BY TO_DATE(opened_at) ORDER BY requester_id, id))) / 60, 1) AS minutes_since_prev_opened_at,
        SIZE(COLLECT_SET(priority) OVER w_agent_tier) AS distinct_priority_in_agent_tier,
        DENSE_RANK() OVER (PARTITION BY TO_DATE(opened_at) ORDER BY requester_id) AS requester_id_dense_rank,
        LAST_VALUE(agent_hired_on) IGNORE NULLS OVER (PARTITION BY agent_tier ORDER BY requester_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_agent_hired_on,
        ROUND(CUME_DIST() OVER w_agent_tier_by_agent_hired_on_2, 3) AS agent_hired_on_cume_dist,
        LEAD(agent_hired_on) OVER w_agent_tier_by_agent_hired_on_3 AS next_agent_hired_on,
        MAX(requester_id) OVER (PARTITION BY agent_tier ORDER BY agent_hired_on DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS requester_id_max_7
    FROM with_parent_agent
    WINDOW w_channel AS (PARTITION BY channel), w_agent_tier AS (PARTITION BY agent_tier), w_agent_tier_by_agent_hired_on_2 AS (PARTITION BY agent_tier ORDER BY agent_hired_on DESC), w_agent_tier_by_agent_hired_on_3 AS (PARTITION BY agent_tier ORDER BY agent_hired_on DESC, id)
),
top_per_group AS (
    -- Top 3 per group
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        agent_tier,
        agent_hired_on,
        requester_id_vs_channel_avg,
        running_requester_id,
        rows_in_channel,
        first_agent_hired_on,
        days_since_prev_agent_hired_on,
        second_closed_at,
        agent_tier_min_requester_id,
        minutes_since_prev_opened_at,
        distinct_priority_in_agent_tier,
        last_known_agent_hired_on,
        agent_hired_on_cume_dist,
        next_agent_hired_on,
        requester_id_max_7
    FROM with_windows
    WHERE requester_id_dense_rank <= 3
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(requester_id_vs_channel_avg) OVER (ORDER BY requester_id DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS requester_id_vs_channel_avg_max_2_prior,
        FIRST_VALUE(rows_in_channel) IGNORE NULLS OVER (PARTITION BY channel, agent_tier ORDER BY requester_id_vs_channel_avg NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_rows_in_channel,
        MAX(agent_tier_min_requester_id) OVER (PARTITION BY TO_DATE(second_closed_at)) AS second_closed_at_to_date_max_agent_tier_min_requ,
        FIRST_VALUE(agent_hired_on) IGNORE NULLS OVER (ORDER BY requester_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_agent_hired_on,
        COUNT(*) OVER (PARTITION BY channel, agent_tier ORDER BY requester_id_vs_channel_avg NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        LEAD(requester_id_max_7, 3) OVER (PARTITION BY channel, agent_tier ORDER BY requester_id_vs_channel_avg NULLS LAST, id) AS next3_requester_id_max_7,
        SIZE(COLLECT_SET(priority) OVER w_second_closed_at_to_date) AS distinct_priority_in_second_closed_at_to_date,
        SUM(requester_id) OVER (PARTITION BY TO_DATE(second_closed_at)) AS second_closed_at_to_date_sum_requester_id
    FROM top_per_group
    WINDOW w_second_closed_at_to_date AS (PARTITION BY TO_DATE(second_closed_at))
),
stats AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', first_agent_hired_on) AS first_agent_hired_on_month,
        DATE_TRUNC('MONTH', opened_at) AS opened_at_month,
        ARRAY_SORT(COLLECT_SET(channel)) AS channel_list,
        MAX(last_known_agent_hired_on) AS last_last_known_agent_hired_on
    FROM windowed
    GROUP BY DATE_TRUNC('MONTH', first_agent_hired_on), DATE_TRUNC('MONTH', opened_at)
    HAVING SUM(agent_tier_min_requester_id) > 63
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(CUME_DIST() OVER w_all_by_last_last_known_agent, 3) AS last_last_known_agent_hired_on_cume_dist,
        ROUND(PERCENT_RANK() OVER w_last_last_known_agent_hired, 3) AS last_last_known_agent_hired_on_percent_rank,
        LAST_VALUE(first_agent_hired_on_month) OVER (PARTITION BY DATE_TRUNC('MONTH', last_last_known_agent_hired_on) ORDER BY last_last_known_agent_hired_on ASC NULLS LAST, first_agent_hired_on_month, opened_at_month ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_first_agent_hired_on_month,
        FIRST_VALUE(opened_at_month) IGNORE NULLS OVER (ORDER BY last_last_known_agent_hired_on ASC, first_agent_hired_on_month, opened_at_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_opened_at_month
    FROM stats
    WINDOW w_all_by_last_last_known_agent AS (ORDER BY last_last_known_agent_hired_on ASC, first_agent_hired_on_month, opened_at_month), w_last_last_known_agent_hired AS (PARTITION BY DATE_TRUNC('MONTH', last_last_known_agent_hired_on) ORDER BY last_last_known_agent_hired_on ASC NULLS LAST)
)
-- Final projection
SELECT
    first_agent_hired_on_month,
    channel_list,
    last_last_known_agent_hired_on,
    last_last_known_agent_hired_on_cume_dist,
    last_first_agent_hired_on_month,
    first_known_opened_at_month,
    DATE_FORMAT(opened_at_month, 'yyyy-MM-dd HH:00') AS opened_at_month_label
FROM ranked
WHERE last_last_known_agent_hired_on_cume_dist BETWEEN 0.1 AND 0.8
ORDER BY last_last_known_agent_hired_on DESC NULLS FIRST
