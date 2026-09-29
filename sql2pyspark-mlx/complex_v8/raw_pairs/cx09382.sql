WITH recent AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        channel,
        tags,
        CONCAT(channel, '#', CAST(agent_id AS STRING)) AS channel_ref
    FROM tickets
    WHERE ARRAY_CONTAINS(tags, 'outage')
),
agent_id_stats AS (
    -- Avg requester_id per agent_id
    SELECT agent_id, AVG(requester_id) AS avg_requester_id
    FROM recent
    GROUP BY agent_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_requester_id,
        ROUND(s.requester_id - g.avg_requester_id, 2) AS requester_id_minus_avg
    FROM recent s
    JOIN agent_id_stats g ON g.agent_id = s.agent_id
    WHERE s.requester_id > g.avg_requester_id
),
screened AS (
    -- Rows with no matching tickets
    SELECT s.*
    FROM outliers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND t.requester_id < 16
        )
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        requester_id,
        agent_id,
        channel,
        tags,
        avg_requester_id,
        requester_id_minus_avg,
        ABS(requester_id_minus_avg - 10.0) AS requester_id_minus_avg_distance,
        CASE WHEN channel IN ('chat', 'web') THEN 'group_a' WHEN channel = 'phone' THEN 'group_b' ELSE 'other' END AS channel_group,
        requester_id_minus_avg + avg_requester_id AS requester_id_minus_avg_plus_avg_requester_id
    FROM screened
    WHERE requester_id_minus_avg > -7.5
)
-- Final projection
SELECT
    id,
    requester_id,
    agent_id,
    channel,
    tags,
    avg_requester_id,
    requester_id_minus_avg,
    requester_id_minus_avg_distance,
    channel_group,
    requester_id_minus_avg_plus_avg_requester_id,
    requester_id_minus_avg + avg_requester_id AS requester_id_minus_avg_plus_avg_requester_id_2,
    LEAST(avg_requester_id, requester_id_minus_avg) AS least_avg_requester_id_requester_id_minus_avg
FROM derived
WHERE channel_group IN ('group_b', 'other')
ORDER BY requester_id_minus_avg_distance NULLS LAST
