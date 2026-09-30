WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        priority,
        tags,
        COALESCE(requester_id, 0) AS requester_id_filled,
        CONCAT_WS('|', priority, channel) AS priority_channel_label,
        CONCAT(channel, '#', CAST(csat AS STRING)) AS channel_ref
    FROM tickets
    WHERE (priority <> 'P2' OR requester_id <= 19)
),
with_counts AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM tickets t WHERE t.agent_id = s.agent_id) AS ticket_count,
        (SELECT COUNT(CASE WHEN t.priority IN ('P3', 'P4') THEN 1 END) FROM tickets t WHERE t.agent_id = s.agent_id) AS ticket_priority_count
    FROM prepared s
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        requester_id,
        priority,
        requester_id_filled,
        priority_channel_label,
        channel_ref,
        COUNT(*) OVER (PARTITION BY agent_id, priority) AS rows_in_agent_id_priority,
        ROW_NUMBER() OVER (PARTITION BY ticket_count ORDER BY requester_id_filled, id) AS rn,
        NTH_VALUE(ticket_count, 2) OVER (PARTITION BY ticket_count ORDER BY requester_id_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ticket_count,
        FIRST_VALUE(ticket_count) IGNORE NULLS OVER (PARTITION BY ticket_count ORDER BY requester_id_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_ticket_count
    FROM with_counts
    WINDOW w_agent_id_priority AS (PARTITION BY agent_id, priority)
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MAX(second_ticket_count) OVER (PARTITION BY second_ticket_count) AS second_ticket_count_max_second_ticket_count,
        LEAD(requester_id) OVER w_all_by_requester_id AS next_requester_id,
        LAST_VALUE(rn) OVER (ORDER BY requester_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_rn,
        MIN(requester_id) OVER w_second_ticket_count AS second_ticket_count_min_requester_id,
        NTH_VALUE(rn, 2) OVER (PARTITION BY rn ORDER BY first_known_ticket_count NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_rn,
        SIZE(COLLECT_SET(priority) OVER (PARTITION BY second_ticket_count)) AS distinct_priority_in_second_ticket_count
    FROM with_trends
    WINDOW w_all_by_requester_id AS (ORDER BY requester_id DESC, id), w_second_ticket_count AS (PARTITION BY second_ticket_count)
)
-- Subtotals with CUBE
SELECT
    first_known_ticket_count,
    rn,
    COALESCE(priority, 'ALL') AS priority,
    GROUPING_ID() AS gid,
    ARRAY_SORT(COLLECT_SET(priority)) AS priority_list,
    COUNT(DISTINCT priority) AS distinct_prioritys
FROM with_windows
GROUP BY CUBE(first_known_ticket_count, rn, priority)
ORDER BY first_known_ticket_count NULLS LAST, rn NULLS LAST, priority NULLS LAST
