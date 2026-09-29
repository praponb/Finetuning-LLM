WITH recent_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        channel,
        tags,
        QUARTER(closed_at) AS closed_at_quarter,
        LENGTH(TRIM(channel)) AS channel_len
    FROM tickets
    WHERE SIZE(tags) >= 2
),
priority_stats AS (
    -- Avg requester_id per priority
    SELECT priority, AVG(requester_id) AS avg_requester_id
    FROM recent_tickets
    GROUP BY priority
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_requester_id,
        ROUND(s.requester_id - g.avg_requester_id, 2) AS requester_id_minus_avg
    FROM recent_tickets s
    JOIN priority_stats g ON g.priority = s.priority
    WHERE s.requester_id > g.avg_requester_id
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTH_VALUE(requester_id, 2) OVER (PARTITION BY closed_at_quarter ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_requester_id,
        MIN(requester_id) OVER (PARTITION BY closed_at_quarter, channel) AS closed_at_quarter_channel_min_requester_id,
        SUM(requester_id) OVER (PARTITION BY closed_at_quarter ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_requester_id
    FROM outliers
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT closed_at_quarter, priority, channel_len
    FROM with_trends
) src
PIVOT (
    COUNT(channel_len)
    FOR priority IN ('P2', 'P3', 'P4', 'P1')
)
ORDER BY closed_at_quarter
