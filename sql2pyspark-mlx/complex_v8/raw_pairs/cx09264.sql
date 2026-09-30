WITH prepared_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        priority,
        csat,
        tags,
        LENGTH(TRIM(channel)) AS channel_len
    FROM tickets
    WHERE channel = 'email'
),
agent_id_stats AS (
    -- Avg channel_len per agent_id
    SELECT agent_id, AVG(channel_len) AS avg_channel_len
    FROM prepared_tickets
    GROUP BY agent_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_channel_len,
        ROUND(s.channel_len - g.avg_channel_len, 2) AS channel_len_minus_avg
    FROM prepared_tickets s
    JOIN agent_id_stats g ON g.agent_id = s.agent_id
    WHERE s.channel_len > g.avg_channel_len
)
SELECT id, metric, metric_value
FROM (
    SELECT id, channel_len_minus_avg, avg_channel_len
    FROM vs_group
) src
UNPIVOT (
    metric_value FOR metric IN (channel_len_minus_avg, avg_channel_len)
) unp
ORDER BY id, metric
