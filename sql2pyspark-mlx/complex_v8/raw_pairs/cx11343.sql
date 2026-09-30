WITH base AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        CASE WHEN priority IN ('P3', 'P4') THEN 'group_a' WHEN priority = 'P2' THEN 'group_b' ELSE 'other' END AS priority_group,
        INITCAP(TRIM(channel)) AS channel_clean
    FROM tickets
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.priority,
        s.priority_group,
        s.channel_clean,
        su.agent_name,
        su.team AS agent_team,
        su.hired_on AS agent_hired_on
    FROM base s
    JOIN support_agents su ON su.id = s.agent_id
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTH_VALUE(opened_at, 2) OVER (PARTITION BY agent_id, priority ORDER BY requester_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_opened_at,
        SIZE(COLLECT_SET(priority) OVER (PARTITION BY DATE_TRUNC('MONTH', agent_hired_on), priority_group)) AS distinct_priority_in_agent_hired_on_month_priori,
        SUM(requester_id) OVER (PARTITION BY agent_id, priority ORDER BY requester_id DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS requester_id_sum_3_prior,
        RANK() OVER (PARTITION BY agent_id, priority ORDER BY requester_id DESC) AS requester_id_rank,
        FIRST_VALUE(requester_id) OVER (PARTITION BY agent_id, priority ORDER BY requester_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_requester_id,
        FIRST_VALUE(agent_hired_on) IGNORE NULLS OVER (PARTITION BY agent_id ORDER BY closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_agent_hired_on,
        RANK() OVER (ORDER BY agent_hired_on, id) AS agent_hired_on_rank,
        RANK() OVER (ORDER BY agent_hired_on) AS agent_hired_on_rank_2,
        COUNT(*) OVER (PARTITION BY agent_id, priority) AS rows_in_agent_id_priority
    FROM with_details
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        ABS(requester_id_sum_3_prior - 27) AS requester_id_sum_3_prior_distance,
        DATE_TRUNC('DAY', closed_at) AS closed_at_day,
        LENGTH(TRIM(agent_team)) AS agent_team_len
    FROM sequenced
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        DATEDIFF(first_known_agent_hired_on, LAG(first_known_agent_hired_on) OVER (PARTITION BY agent_team_len, MONTH(opened_at) ORDER BY second_opened_at DESC, id)) AS days_since_prev_first_known_agent_hired_on,
        SIZE(COLLECT_SET(priority) OVER (PARTITION BY distinct_priority_in_agent_hired_on_month_priori, agent_id)) AS distinct_priority_in_distinct_priority_in_agent_,
        MIN(rows_in_agent_id_priority) OVER w_distinct_priority_in_agent_h AS distinct_priority_in_agent_hired_on_month_priori_2
    FROM scored
    WINDOW w_distinct_priority_in_agent_h AS (PARTITION BY distinct_priority_in_agent_hired_on_month_priori, agent_id)
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        YEAR(opened_at) AS opened_at_year,
        distinct_priority_in_distinct_priority_in_agent_,
        ROUND(STDDEV(requester_id_rank), 2) AS stddev_requester_id_rank,
        ARRAY_SORT(COLLECT_SET(agent_team)) AS agent_team_list,
        MIN(requester_id_sum_3_prior) AS min_requester_id_sum_3_prior
    FROM windowed
    GROUP BY 1, 2
    HAVING AVG(distinct_priority_in_agent_hired_on_month_priori_2) > 71
)
-- Final projection
SELECT
    opened_at_year,
    distinct_priority_in_distinct_priority_in_agent_,
    agent_team_list
FROM per_group
ORDER BY distinct_priority_in_distinct_priority_in_agent_
