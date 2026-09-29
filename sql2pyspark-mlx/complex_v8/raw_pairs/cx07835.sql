WITH filtered_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        channel,
        tags,
        DATE_TRUNC('HOUR', closed_at) AS closed_at_hour,
        SIZE(tags) AS tags_count,
        LENGTH(TRIM(channel)) AS channel_len
    FROM tickets
    WHERE channel IS NOT NULL
),
filtered AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM filtered_tickets s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND t.csat <= 4
              AND HOUR(t.opened_at) BETWEEN 7 AND 11
        )
      AND SIZE(s.tags) >= 2
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        ABS(tags_count - 2) AS tags_count_distance,
        ROUND(requester_id / NULLIF(tags_count, 0), 3) AS requester_id_per_tags_count,
        DATEDIFF(DATE '2025-12-31', closed_at) AS days_since_closed_at
    FROM filtered
    WHERE agent_id >= 8
),
latest_per_key AS (
    -- Keep the latest row per channel, agent_id
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        channel,
        tags,
        closed_at_hour,
        tags_count,
        channel_len,
        tags_count_distance,
        requester_id_per_tags_count,
        days_since_closed_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY channel, agent_id ORDER BY channel_len DESC, id DESC) AS row_num
        FROM labeled
    ) ranked
    WHERE row_num = 1
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT closed_at, agent_id, channel, channel_len
    FROM latest_per_key
) src
PIVOT (
    MAX(channel_len)
    FOR channel IN ('chat', 'phone')
)
