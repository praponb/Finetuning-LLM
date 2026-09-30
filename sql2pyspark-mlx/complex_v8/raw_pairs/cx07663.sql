WITH scoped AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        channel,
        csat,
        tags
    FROM tickets
    WHERE channel = 'phone'
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(requester_id) OVER (ORDER BY csat, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS requester_id_avg_3,
        RANK() OVER (ORDER BY csat, id) AS csat_rank,
        SIZE(COLLECT_SET(priority) OVER (PARTITION BY agent_id, channel)) AS distinct_priority_in_agent_id_channel
    FROM scoped
),
filtered AS (
    -- Compare against the overall max
    SELECT *
    FROM windowed
    WHERE requester_id < (SELECT MAX(requester_id) FROM windowed)
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        channel,
        csat,
        tags,
        distinct_priority_in_agent_id_channel,
        SUM(requester_id_avg_3) OVER (ORDER BY csat_rank DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_requester_id_avg_3,
        ROUND((UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(LAG(opened_at) OVER (ORDER BY csat_rank DESC, id))) / 60, 1) AS minutes_since_prev_opened_at
    FROM filtered
)
-- Final projection
SELECT
    id,
    requester_id,
    opened_at,
    priority,
    channel,
    csat,
    tags,
    distinct_priority_in_agent_id_channel
FROM ranked
ORDER BY priority DESC, channel
