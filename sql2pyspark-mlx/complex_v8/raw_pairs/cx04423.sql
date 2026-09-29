WITH valid AS (
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
        tags,
        CONCAT(priority, '#', CAST(csat AS STRING)) AS priority_ref,
        CONCAT_WS(' / ', channel, priority) AS channel_priority_label
    FROM tickets
    WHERE (csat >= 1 OR channel <> 'phone')
),
totals AS (
    -- Group-level totals
    SELECT
        channel,
        csat,
        agent_id,
        ARRAY_SORT(COLLECT_SET(channel)) AS channel_list,
        SUM(requester_id) AS total_requester_id,
        MAX(csat) AS max_csat
    FROM valid
    GROUP BY channel, csat, agent_id
),
filtered AS (
    -- Keep rows whose agent matches
    SELECT s.*
    FROM totals s
    WHERE NOT EXISTS (
            SELECT 1
            FROM support_agents su
            WHERE su.id = s.agent_id
              AND su.tier IS NOT NULL
        )
)
SELECT channel, csat, measure, reading
FROM (
    SELECT channel, csat, max_csat, total_requester_id
    FROM filtered
) src
UNPIVOT (
    reading FOR measure IN (max_csat, total_requester_id)
) unp
