WITH cleaned AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        channel,
        csat,
        tags,
        ROUND(requester_id / NULLIF(csat, 0), 3) AS requester_id_per_csat,
        SUBSTRING(TRIM(channel), 1, 2) AS channel_prefix
    FROM tickets
    WHERE requester_id <= 20
),
channel_stats AS (
    -- Avg requester_id per channel
    SELECT channel, AVG(requester_id) AS avg_requester_id
    FROM cleaned
    GROUP BY channel
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_requester_id,
        ROUND(s.requester_id - g.avg_requester_id, 2) AS requester_id_minus_avg
    FROM cleaned s
    JOIN channel_stats g ON g.channel = s.channel
    WHERE s.requester_id > g.avg_requester_id
),
joined_agent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.priority,
        s.channel,
        s.csat,
        s.tags,
        s.requester_id_per_csat,
        s.avg_requester_id,
        s.requester_id_minus_avg,
        su.tier AS agent_tier,
        su.agent_name
    FROM above_average s
    JOIN support_agents su ON su.id = s.agent_id
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric, amount
FROM (
    SELECT id, avg_requester_id, requester_id_minus_avg, requester_id_per_csat
    FROM joined_agent
) src
UNPIVOT (
    amount FOR metric IN (avg_requester_id AS avg_requester_id_v, requester_id_minus_avg AS m_requester_id_minus_avg, requester_id_per_csat AS requester_id_per_csat_v)
) unp
