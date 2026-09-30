WITH valid AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        CASE WHEN channel IN ('email', 'web') THEN 'group_a' WHEN channel = 'phone' THEN 'group_b' ELSE 'other' END AS channel_group
    FROM tickets
    WHERE requester_id <= 17
      AND NOT ARRAY_CONTAINS(tags, 'login')
),
derived AS (
    -- Add calculated columns
    SELECT *, MONTH(closed_at) AS closed_at_month, ABS(csat - 2) AS csat_distance
    FROM valid
    WHERE SIZE(tags) >= 2
),
unique_rows AS (
    -- Keep the latest row per channel
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        channel_group,
        closed_at_month,
        csat_distance
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY channel ORDER BY closed_at_month DESC, id DESC) AS row_num
        FROM derived
    ) ranked
    WHERE row_num = 1
)
SELECT
    id,
    agent_id,
    closed_at,
    tags,
    closed_at_month,
    csat_distance,
    ROUND(csat / NULLIF(requester_id, 0), 3) AS csat_per_requester_id
FROM unique_rows
ORDER BY id
LIMIT 10
