WITH recent AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        CONCAT(priority, '#', CAST(csat AS STRING)) AS priority_ref,
        SUBSTRING(TRIM(priority), 1, 2) AS priority_prefix,
        ABS(requester_id - 8) AS requester_id_distance
    FROM tickets
    WHERE TO_DATE(closed_at) < DATE '2025-03-14'
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SIZE(COLLECT_SET(channel) OVER w_agent_id) AS distinct_channel_in_agent_id,
        DENSE_RANK() OVER (PARTITION BY channel, agent_id ORDER BY closed_at ASC, id) AS closed_at_dense_rank,
        SUM(csat) OVER (PARTITION BY MONTH(closed_at), agent_id ORDER BY csat ASC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS csat_sum_3_prior,
        COUNT(*) OVER w_agent_id AS rows_in_agent_id,
        SUM(requester_id_distance) OVER (PARTITION BY agent_id) AS agent_id_sum_requester_id_distance,
        ROUND(requester_id_distance / NULLIF(SUM(requester_id_distance) OVER (PARTITION BY agent_id), 0), 4) AS requester_id_distance_share_of_agent_id
    FROM recent
    WINDOW w_agent_id AS (PARTITION BY agent_id)
),
combined_details AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.csat,
        s.tags,
        s.priority_prefix,
        s.requester_id_distance,
        s.distinct_channel_in_agent_id,
        s.closed_at_dense_rank,
        s.csat_sum_3_prior,
        s.agent_id_sum_requester_id_distance,
        s.requester_id_distance_share_of_agent_id,
        su.team AS agent_team
    FROM with_trends s
    LEFT JOIN support_agents su ON su.id = s.agent_id
)
SELECT
    agent_team,
    distinct_channel_in_agent_id,
    GROUPING_ID() AS gid,
    COUNT(DISTINCT id) AS distinct_ids
FROM combined_details
GROUP BY ROLLUP(agent_team, distinct_channel_in_agent_id)
