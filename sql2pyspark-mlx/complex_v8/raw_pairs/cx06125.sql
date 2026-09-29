WITH scoped AS (
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
    WHERE channel NOT IN ('email', 'web')
),
stats AS (
    -- Group-level totals
    SELECT
        channel,
        agent_id,
        ARRAY_SORT(COLLECT_SET(priority)) AS priority_list,
        SUM(CASE WHEN channel IS NOT NULL THEN requester_id ELSE 0 END) AS requester_id_channel_is_not_null,
        COUNT(DISTINCT channel) AS distinct_channels,
        SUM(requester_id) AS total_requester_id
    FROM scoped
    GROUP BY channel, agent_id
)
SELECT *
FROM (
    SELECT distinct_channels, agent_id, channel, requester_id_channel_is_not_null
    FROM stats
) src
PIVOT (
    COUNT(requester_id_channel_is_not_null) AS count_v, SUM(requester_id_channel_is_not_null) AS sum_v
    FOR channel IN ('phone', 'email', 'chat')
)
