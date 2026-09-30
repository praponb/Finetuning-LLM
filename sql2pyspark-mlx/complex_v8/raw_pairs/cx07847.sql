WITH recent_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        channel,
        csat,
        CONCAT_WS(' / ', priority, channel) AS priority_channel_label
    FROM tickets
    WHERE csat >= 1
),
stats AS (
    -- Summarise per group
    SELECT
        csat,
        channel,
        agent_id,
        ROUND(100 * COUNT(CASE WHEN channel <> 'chat' THEN 1 END) / COUNT(*), 1) AS pct_channel_chat,
        MAX(opened_at) AS last_opened_at,
        COUNT(DISTINCT id) AS distinct_ids,
        ROUND(SUM(requester_id) / NULLIF(SUM(csat), 0), 3) AS requester_id_to_csat_ratio,
        MIN(opened_at) AS first_opened_at,
        COUNT(*) AS cnt
    FROM recent_tickets
    WHERE channel = 'chat'
    GROUP BY 1, 2, 3
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT
        agent_id,
        MIN(opened_at) AS first_ticket_opened_at,
        COUNT(*) AS ticket_count
    FROM tickets
    GROUP BY agent_id
),
profiled AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        t.first_ticket_opened_at,
        COALESCE(t.ticket_count, 0) AS ticket_count
    FROM stats s
    LEFT JOIN ticket_stats t ON t.agent_id = s.agent_id
)
-- Final projection
SELECT
    csat,
    channel,
    agent_id,
    pct_channel_chat,
    last_opened_at,
    distinct_ids,
    cnt,
    first_ticket_opened_at,
    ticket_count
FROM profiled
ORDER BY last_opened_at, csat, channel, agent_id
LIMIT 20
