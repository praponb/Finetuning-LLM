WITH valid_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        opened_at,
        channel,
        tags,
        FLOOR(requester_id / 5) * 5 AS requester_id_bucket
    FROM tickets
    WHERE channel IS NOT NULL
      AND closed_at < TIMESTAMP '2025-02-26 16:00:00'
),
labeled AS (
    -- Label and score rows
    SELECT *, ABS(requester_id_bucket - 18) AS requester_id_bucket_distance
    FROM valid_tickets
    WHERE channel = 'phone'
)
SELECT *
FROM (
    SELECT opened_at, channel, requester_id_bucket_distance
    FROM labeled
) src
PIVOT (
    COUNT(requester_id_bucket_distance)
    FOR channel IN ('web', 'phone')
)
ORDER BY opened_at
