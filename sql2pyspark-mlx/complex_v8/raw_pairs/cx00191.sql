WITH filtered_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        csat,
        tags,
        CONCAT_WS('-', channel, priority) AS channel_priority_label,
        CONCAT_WS(': ', channel, priority) AS channel_priority_label_2,
        FLOOR(requester_id / 5) * 5 AS requester_id_bucket
    FROM tickets
    WHERE requester_id >= 5
),
joined AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.closed_at,
        s.csat,
        s.tags,
        s.channel_priority_label,
        s.channel_priority_label_2,
        s.requester_id_bucket,
        su.hired_on AS agent_hired_on
    FROM filtered_tickets s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.closed_at,
        s.csat,
        s.tags,
        s.channel_priority_label_2,
        s.requester_id_bucket,
        su.tier AS agent_tier,
        su.team AS agent_team
    FROM joined s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
with_details_agent AS (
    -- Attach agent attributes
    SELECT s.*, su.hired_on AS agent_hired_on
    FROM enriched s
    JOIN support_agents su ON su.id = s.agent_id
),
deduped AS (
    -- Keep the earliest row per agent_team
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        csat,
        tags,
        channel_priority_label_2,
        requester_id_bucket,
        agent_tier,
        agent_team,
        agent_hired_on
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY agent_team ORDER BY closed_at, id) AS row_num
        FROM with_details_agent
    ) ranked
    WHERE row_num = 1
)
SELECT
    id,
    requester_id,
    closed_at,
    csat,
    tags,
    channel_priority_label_2,
    requester_id_bucket,
    agent_tier,
    agent_team,
    agent_hired_on,
    ABS(requester_id_bucket - 15) AS requester_id_bucket_distance,
    CASE WHEN agent_tier IN ('T3', 'T2') THEN 'group_a' WHEN agent_tier = 'T1' THEN 'group_b' ELSE 'other' END AS agent_tier_group
FROM deduped
