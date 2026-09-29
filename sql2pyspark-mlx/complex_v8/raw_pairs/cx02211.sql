WITH prepared AS (
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
    WHERE (agent_id <= 9 OR priority IS NOT NULL)
),
with_details_agent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.closed_at,
        s.channel,
        s.csat,
        su.team AS agent_team,
        su.agent_name
    FROM prepared s
    JOIN support_agents su ON su.id = s.agent_id
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(csat) OVER (PARTITION BY channel, agent_team ORDER BY requester_id NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_csat_so_far,
        MIN(csat) OVER (PARTITION BY csat, channel) AS csat_channel_min_csat,
        FIRST_VALUE(closed_at) OVER (PARTITION BY agent_team, channel ORDER BY csat, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_closed_at,
        ROUND(csat / NULLIF(SUM(csat) OVER (PARTITION BY csat, channel), 0), 4) AS csat_share_of_csat_channel,
        ROUND(AVG(csat) OVER (PARTITION BY channel, agent_team ORDER BY requester_id NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS csat_avg_4,
        SUM(requester_id) OVER (PARTITION BY DATE_TRUNC('MONTH', closed_at)) AS closed_at_month_sum_requester_id,
        ROUND(AVG(requester_id) OVER (PARTITION BY agent_team, channel ORDER BY csat, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS requester_id_avg_3_prior,
        ROUND(requester_id - AVG(requester_id) OVER w_closed_at_month, 2) AS requester_id_vs_closed_at_month_avg,
        LAG(requester_id, 2) OVER w_agent_team_channel_by_csat AS prev2_requester_id,
        csat - LAG(csat) OVER (ORDER BY closed_at DESC, id) AS csat_change
    FROM with_details_agent
    WINDOW w_closed_at_month AS (PARTITION BY DATE_TRUNC('MONTH', closed_at)), w_agent_team_channel_by_csat AS (PARTITION BY agent_team, channel ORDER BY csat, id)
),
derived AS (
    -- Label and score rows
    SELECT *, LOWER(TRIM(channel)) AS channel_clean
    FROM sequenced
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LEAD(csat_change) OVER (ORDER BY first_closed_at ASC, id) AS next_csat_change,
        COUNT(*) OVER (PARTITION BY csat, agent_team) AS rows_in_csat_agent_team,
        FIRST_VALUE(prev2_requester_id) OVER (ORDER BY first_closed_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_prev2_requester_id,
        MAX(csat_avg_4) OVER (ORDER BY first_closed_at ASC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS csat_avg_4_max_3
    FROM derived
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        agent_team,
        MAX(first_closed_at) AS last_first_closed_at,
        SUM(next_csat_change) AS total_next_csat_change,
        ROUND(SUM(csat_avg_4_max_3) / NULLIF(SUM(closed_at_month_sum_requester_id), 0), 3) AS csat_avg_4_max_3_to_closed_at_month_sum_requeste,
        SUM(requester_id_avg_3_prior) AS total_requester_id_avg_3_prior,
        ROUND(STDDEV(csat_avg_4), 2) AS stddev_csat_avg_4,
        ROUND(AVG(max_csat_so_far), 2) AS avg_max_csat_so_far
    FROM with_trends
    WHERE next_csat_change > -4
    GROUP BY agent_team
)
-- Final projection
SELECT
    agent_team,
    last_first_closed_at,
    total_next_csat_change,
    csat_avg_4_max_3_to_closed_at_month_sum_requeste,
    total_requester_id_avg_3_prior,
    stddev_csat_avg_4,
    avg_max_csat_so_far,
    CASE WHEN agent_team IN ('Onboarding', 'Tech') THEN 'group_a' WHEN agent_team = 'Billing' THEN 'group_b' ELSE 'other' END AS agent_team_group,
    ROUND(total_next_csat_change / NULLIF(avg_max_csat_so_far, 0), 3) AS total_next_csat_change_per_avg_max_csat_so_far
FROM per_group
ORDER BY agent_team NULLS LAST
