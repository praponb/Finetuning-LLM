WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        csat,
        tags,
        CONCAT(priority, '#', CAST(csat AS STRING)) AS priority_ref,
        COALESCE(channel, 'UNKNOWN') AS channel_or_unknown
    FROM tickets
    WHERE ARRAY_CONTAINS(tags, 'login')
),
latest_per_key AS (
    -- Keep the latest row per channel, channel_or_unknown
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        csat,
        tags,
        priority_ref,
        channel_or_unknown
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY channel, channel_or_unknown ORDER BY requester_id DESC, id DESC) AS rn
        FROM filtered
    ) ranked
    WHERE rn = 1
),
with_activity AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM latest_per_key s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND t.channel NOT IN ('email', 'phone')
        )
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.tags,
        s.priority_ref,
        s.channel_or_unknown,
        su.team AS agent_team,
        su.tier AS agent_tier,
        su.agent_name
    FROM with_activity s
    LEFT JOIN support_agents su ON su.id = s.agent_id AND su.tier IN ('T1', 'T3')
),
scored AS (
    -- Derive helper columns
    SELECT *, DATE_TRUNC('DAY', opened_at) AS opened_at_day
    FROM joined
    WHERE channel_or_unknown = 'chat'
)
-- Final projection
SELECT
    id,
    agent_id,
    tags,
    priority_ref,
    channel_or_unknown,
    agent_team,
    agent_name,
    opened_at_day,
    ABS(requester_id - 16) AS requester_id_distance,
    LENGTH(TRIM(agent_team)) AS agent_team_len
FROM scored
ORDER BY agent_team DESC
