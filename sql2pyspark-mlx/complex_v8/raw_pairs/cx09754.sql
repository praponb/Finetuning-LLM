WITH recent AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        channel,
        CONCAT_WS(' / ', channel, priority) AS channel_priority_label,
        CONCAT_WS(' / ', priority, channel) AS priority_channel_label
    FROM tickets
    WHERE priority IS NOT NULL
      AND requester_id <= 14
),
enriched_agent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.priority,
        s.channel,
        s.channel_priority_label,
        s.priority_channel_label,
        su.team AS agent_team
    FROM recent s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY channel ORDER BY requester_id DESC, id) AS requester_id_dense_rank,
        COUNT(*) OVER (PARTITION BY channel ORDER BY requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        SUM(requester_id) OVER (PARTITION BY agent_id) AS agent_id_sum_requester_id,
        SIZE(COLLECT_SET(priority) OVER (PARTITION BY agent_id)) AS distinct_priority_in_agent_id
    FROM enriched_agent
)
SELECT
    id,
    requester_id,
    agent_id,
    opened_at,
    priority,
    channel,
    channel_priority_label,
    priority_channel_label,
    agent_team,
    requester_id_dense_rank,
    running_count,
    agent_id_sum_requester_id,
    distinct_priority_in_agent_id,
    FLOOR(requester_id_dense_rank / 2) * 2 AS requester_id_dense_rank_bucket
FROM with_trends
ORDER BY opened_at DESC
