WITH valid_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags
    FROM tickets
),
latest_per_key AS (
    -- Keep the earliest row per priority, agent_id
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY priority, agent_id ORDER BY opened_at, id) AS row_num
        FROM valid_tickets
    ) ranked
    WHERE row_num = 1
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        priority,
        channel,
        COUNT(*) FILTER (WHERE channel IN ('web', 'phone', 'chat')) AS n_channel_in_web_phone_chat,
        MIN(closed_at) AS first_closed_at,
        SUM(csat) AS total_csat,
        MAX(csat) AS max_csat,
        ARRAY_SORT(COLLECT_SET(priority)) AS priority_list,
        MIN(requester_id) AS min_requester_id
    FROM latest_per_key
    GROUP BY priority, channel
)
SELECT
    channel,
    max_csat,
    n_channel_in_web_phone_chat,
    GROUPING_ID() AS grouping_level,
    ARRAY_SORT(COLLECT_SET(channel)) AS channel_list,
    PERCENTILE_APPROX(max_csat, 0.5) AS median_max_csat,
    MAX(min_requester_id) AS max_min_requester_id
FROM stats
GROUP BY GROUPING SETS ((channel, max_csat, n_channel_in_web_phone_chat), (channel), (max_csat), ())
ORDER BY channel NULLS LAST, max_csat NULLS LAST, n_channel_in_web_phone_chat NULLS LAST
