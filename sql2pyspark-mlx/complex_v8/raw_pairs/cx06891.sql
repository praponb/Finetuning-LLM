WITH recent_tickets AS (
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
        tags,
        CASE WHEN priority IN ('P3', 'P2') THEN 'group_a' WHEN priority = 'P4' THEN 'group_b' ELSE 'other' END AS priority_group
    FROM tickets
    WHERE opened_at >= TIMESTAMP '2025-01-13 13:00:00'
),
with_parent_agent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.priority,
        s.channel,
        s.csat,
        su.agent_name
    FROM recent_tickets s
    JOIN support_agents su ON su.id = s.agent_id
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat,
        SUM(csat) OVER (PARTITION BY DATE_TRUNC('MONTH', opened_at), priority ORDER BY requester_id, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS csat_sum_2,
        SUM(requester_id) OVER (PARTITION BY YEAR(opened_at)) AS opened_at_year_sum_requester_id,
        ROUND(csat - AVG(csat) OVER (PARTITION BY YEAR(opened_at)), 2) AS csat_vs_opened_at_year_avg,
        SUM(csat) OVER (ORDER BY opened_at, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS csat_sum_1_prior,
        COUNT(*) OVER (PARTITION BY YEAR(opened_at)) AS rows_in_opened_at_year,
        MIN(csat) OVER (ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_csat_so_far,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY YEAR(opened_at))) AS distinct_agent_id_in_opened_at_year,
        ROUND(PERCENT_RANK() OVER w_opened_at_month_priority_by_2, 3) AS requester_id_percent_rank,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(LAG(closed_at) OVER (ORDER BY opened_at, id))) / 60, 1) AS minutes_since_prev_closed_at,
        ROUND(CUME_DIST() OVER (PARTITION BY DATE_TRUNC('MONTH', opened_at), priority ORDER BY requester_id, id), 3) AS requester_id_cume_dist,
        FIRST_VALUE(closed_at) IGNORE NULLS OVER (ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_closed_at,
        ROUND(requester_id - AVG(requester_id) OVER w_opened_at_year, 2) AS requester_id_vs_opened_at_year_avg,
        FIRST_VALUE(csat) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('MONTH', opened_at), priority ORDER BY requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_csat,
        LAG(requester_id, 3) OVER (PARTITION BY DATE_TRUNC('MONTH', opened_at), priority ORDER BY requester_id, id) AS prev3_requester_id
    FROM with_parent_agent
    WINDOW w_opened_at_month_priority_by_2 AS (PARTITION BY DATE_TRUNC('MONTH', opened_at), priority ORDER BY requester_id, id), w_opened_at_year AS (PARTITION BY YEAR(opened_at))
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat,
        opened_at_year_sum_requester_id,
        csat_vs_opened_at_year_avg,
        csat_sum_1_prior,
        min_csat_so_far,
        requester_id_percent_rank,
        minutes_since_prev_closed_at,
        requester_id_cume_dist,
        first_known_closed_at,
        requester_id_vs_opened_at_year_avg,
        first_known_csat,
        prev3_requester_id,
        CONCAT(channel, '#', CAST(first_known_csat AS STRING)) AS channel_ref,
        ABS(csat_sum_2 - 7) AS csat_sum_2_distance
    FROM with_trends
    WHERE requester_id_cume_dist >= 0.2
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        NTILE(4) OVER (ORDER BY requester_id_percent_rank DESC NULLS FIRST, id) AS requester_id_percent_rank_quartile,
        ROUND(requester_id_cume_dist - AVG(requester_id_cume_dist) OVER (PARTITION BY priority), 2) AS requester_id_cume_dist_vs_priority_avg,
        RANK() OVER (PARTITION BY priority ORDER BY requester_id_percent_rank DESC) AS requester_id_percent_rank_rank
    FROM tagged
)
-- Final projection
SELECT
    id,
    closed_at,
    priority,
    channel,
    csat,
    opened_at_year_sum_requester_id,
    csat_vs_opened_at_year_avg,
    csat_sum_1_prior,
    min_csat_so_far,
    minutes_since_prev_closed_at,
    requester_id_cume_dist,
    requester_id_vs_opened_at_year_avg,
    first_known_csat,
    requester_id_cume_dist_vs_priority_avg,
    requester_id_percent_rank_rank,
    CASE WHEN channel IN ('phone', 'web') THEN 'group_a' WHEN channel = 'email' THEN 'group_b' ELSE 'other' END AS channel_group
FROM windowed
WHERE requester_id_percent_rank_rank >= 2
