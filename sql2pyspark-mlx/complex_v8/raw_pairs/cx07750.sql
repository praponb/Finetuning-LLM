WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(opened_at)) / 3600, 1) AS hours_opened_at_to_closed_at
    FROM tickets
    WHERE priority = 'P2'
      AND SIZE(tags) >= 1
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        UPPER(TRIM(priority)) AS priority_clean,
        CASE WHEN requester_id >= 22 THEN 'HIGH' WHEN requester_id >= 7 THEN 'MEDIUM' ELSE 'LOW' END AS requester_id_tier,
        LOWER(REGEXP_REPLACE(TRIM(channel), ' ', '_')) AS channel_slug
    FROM recent
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(channel_slug), ' ', '_')) AS channel_slug_slug,
        CONCAT_WS('|', priority, requester_id_tier) AS priority_requester_id_tier_label,
        LENGTH(TRIM(channel_slug)) AS channel_slug_len
    FROM tagged
    WHERE requester_id_tier IS NOT NULL
),
with_activity AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM enriched s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND t.channel = 'chat'
        )
)
SELECT
    COALESCE(channel, 'ALL') AS channel,
    COALESCE(requester_id_tier, 'ALL') AS requester_id_tier,
    COALESCE(priority, 'ALL') AS priority,
    COUNT(*) AS cnt,
    SUM(channel_slug_len) AS total_channel_slug_len,
    COUNT(DISTINCT id) AS distinct_ids
FROM with_activity
GROUP BY GROUPING SETS ((channel, requester_id_tier, priority), (channel), (requester_id_tier))
ORDER BY channel NULLS LAST, requester_id_tier NULLS LAST, priority NULLS LAST
