WITH cleaned_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        tags,
        ROUND((UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(closed_at)) / 60, 1) AS minutes_closed_at_to_opened_at,
        ABS(requester_id - 18) AS requester_id_distance
    FROM tickets
    WHERE requester_id <= 22
      AND closed_at < TIMESTAMP '2025-02-21 07:00:00'
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(requester_id) OVER (PARTITION BY priority) AS priority_max_requester_id,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY MONTH(opened_at))) AS distinct_agent_id_in_opened_at_month,
        MAX(requester_id) OVER (ORDER BY closed_at DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS requester_id_max_7,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY priority)) AS distinct_agent_id_in_priority,
        ROUND(requester_id_distance / NULLIF(SUM(requester_id_distance) OVER w_priority, 0), 4) AS requester_id_distance_share_of_priority,
        SUM(requester_id_distance) OVER (PARTITION BY MONTH(opened_at)) AS opened_at_month_sum_requester_id_distance,
        MIN(requester_id) OVER (PARTITION BY agent_id ORDER BY minutes_closed_at_to_opened_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_requester_id_so_far,
        SUM(requester_id) OVER (PARTITION BY MONTH(opened_at)) AS opened_at_month_sum_requester_id,
        MIN(requester_id_distance) OVER w_opened_at_month AS opened_at_month_min_requester_id_distance,
        ROUND(AVG(requester_id_distance) OVER (PARTITION BY priority, agent_id ORDER BY requester_id, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS requester_id_distance_avg_2_prior,
        ROUND(AVG(requester_id) OVER (PARTITION BY priority, agent_id ORDER BY requester_id, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS requester_id_avg_4,
        NTH_VALUE(requester_id, 2) OVER (ORDER BY closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_requester_id,
        SUM(requester_id) OVER (PARTITION BY agent_id ORDER BY requester_id_distance DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_requester_id
    FROM cleaned_tickets
    WINDOW w_priority AS (PARTITION BY priority), w_opened_at_month AS (PARTITION BY MONTH(opened_at))
)
SELECT
    id,
    requester_id,
    opened_at,
    closed_at,
    tags,
    minutes_closed_at_to_opened_at,
    distinct_agent_id_in_opened_at_month,
    requester_id_max_7,
    distinct_agent_id_in_priority,
    requester_id_distance_share_of_priority,
    min_requester_id_so_far,
    opened_at_month_sum_requester_id,
    opened_at_month_min_requester_id_distance,
    second_requester_id,
    running_requester_id,
    ARRAY_CONTAINS(tags, 'bug') AS has_bug,
    CONCAT(priority, '#', CAST(second_requester_id AS STRING)) AS priority_ref
FROM windowed
ORDER BY requester_id_distance_share_of_priority DESC, priority_ref
