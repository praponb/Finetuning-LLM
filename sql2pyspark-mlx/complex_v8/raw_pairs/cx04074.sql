WITH recent_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        tags,
        SUBSTRING(TRIM(channel), 1, 2) AS channel_prefix
    FROM tickets
    WHERE channel IS NOT NULL
      AND agent_id <= 8
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        MONTH(closed_at) AS closed_at_month,
        ARRAY_CONTAINS(tags, 'bug') AS has_bug
    FROM recent_tickets
    WHERE channel_prefix IS NOT NULL
),
latest_per_key AS (
    -- Keep the earliest row per channel
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        tags,
        channel_prefix,
        closed_at_month,
        has_bug
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY channel ORDER BY closed_at_month, id) AS recency_rank
        FROM labeled
    ) ranked
    WHERE recency_rank = 1
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        agent_id,
        has_bug,
        MIN(closed_at) AS first_closed_at,
        ROUND(100 * COUNT(CASE WHEN channel = 'web' THEN 1 END) / COUNT(*), 1) AS pct_channel_web,
        MAX(closed_at) AS last_closed_at,
        COUNT(*) AS n_rows,
        SUM(closed_at_month) AS total_closed_at_month
    FROM latest_per_key
    WHERE closed_at_month > 2
    GROUP BY agent_id, has_bug
    HAVING AVG(closed_at_month) > 3
)
SELECT agent_id, has_bug, measure, value
FROM (
    SELECT agent_id, has_bug, total_closed_at_month, n_rows
    FROM per_group
) src
UNPIVOT (
    value FOR measure IN (total_closed_at_month, n_rows)
) unp
