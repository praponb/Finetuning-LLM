WITH cleaned_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        channel
    FROM tickets
    WHERE csat >= 3
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        channel,
        DENSE_RANK() OVER (PARTITION BY channel, agent_id ORDER BY opened_at DESC, id) AS opened_at_dense_rank,
        SIZE(COLLECT_SET(priority) OVER (PARTITION BY agent_id, channel)) AS distinct_priority_in_agent_id_channel,
        LAST_VALUE(requester_id) OVER (PARTITION BY channel, agent_id ORDER BY opened_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_requester_id,
        ROW_NUMBER() OVER w_channel_agent_id_by_opened_a AS rn,
        ROUND(CUME_DIST() OVER (PARTITION BY channel, agent_id ORDER BY requester_id DESC), 3) AS requester_id_cume_dist,
        MIN(requester_id) OVER (PARTITION BY agent_id, channel) AS agent_id_channel_min_requester_id
    FROM cleaned_tickets
    WINDOW w_channel_agent_id_by_opened_a AS (PARTITION BY channel, agent_id ORDER BY opened_at DESC, id)
)
-- Subtotals with CUBE
SELECT
    distinct_priority_in_agent_id_channel,
    COALESCE(channel, 'ALL') AS channel,
    opened_at_dense_rank,
    COUNT(DISTINCT priority) AS distinct_prioritys,
    ROUND(SUM(opened_at_dense_rank) / NULLIF(SUM(agent_id_channel_min_requester_id), 0), 3) AS opened_at_dense_rank_to_agent_id_channel_min_req,
    ROUND(SUM(requester_id) / NULLIF(SUM(distinct_priority_in_agent_id_channel), 0), 3) AS requester_id_to_distinct_priority_in_agent_id_ch
FROM with_windows
GROUP BY CUBE(distinct_priority_in_agent_id_channel, channel, opened_at_dense_rank)
ORDER BY distinct_priority_in_agent_id_channel NULLS LAST, channel NULLS LAST, opened_at_dense_rank NULLS LAST
