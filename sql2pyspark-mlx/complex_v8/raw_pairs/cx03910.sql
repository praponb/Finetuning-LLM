WITH scoped AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        csat,
        tags,
        CONCAT_WS(': ', priority, channel) AS priority_channel_label,
        DATEDIFF(DATE '2025-12-31', opened_at) AS days_since_opened_at
    FROM tickets
    WHERE opened_at >= TIMESTAMP '2025-02-06 12:00:00'
),
unique_rows AS (
    -- Keep the latest row per priority, agent_id
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        csat,
        tags,
        priority_channel_label,
        days_since_opened_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY priority, agent_id ORDER BY requester_id DESC, id DESC) AS rn
        FROM scoped
    ) ranked
    WHERE rn = 1
),
priority_stats AS (
    -- Avg days_since_opened_at per priority
    SELECT priority, AVG(days_since_opened_at) AS avg_days_since_opened_at
    FROM unique_rows
    GROUP BY priority
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_days_since_opened_at,
        ROUND(s.days_since_opened_at - g.avg_days_since_opened_at, 2) AS days_since_opened_at_minus_avg
    FROM unique_rows s
    JOIN priority_stats g ON g.priority = s.priority
    WHERE s.days_since_opened_at > g.avg_days_since_opened_at
)
-- Final projection
SELECT
    id,
    requester_id,
    agent_id,
    opened_at,
    priority,
    csat,
    tags,
    days_since_opened_at,
    avg_days_since_opened_at,
    days_since_opened_at_minus_avg
FROM above_average
