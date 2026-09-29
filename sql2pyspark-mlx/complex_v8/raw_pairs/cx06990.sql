WITH recent AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        channel,
        tags,
        DATEDIFF(DATE '2025-12-31', closed_at) AS days_since_closed_at
    FROM tickets
    WHERE requester_id >= 4
),
unique_rows AS (
    -- Keep the earliest row per agent_id
    SELECT
        id,
        requester_id,
        agent_id,
        channel,
        tags,
        days_since_closed_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY agent_id ORDER BY days_since_closed_at, id) AS recency_rank
        FROM recent
    ) ranked
    WHERE recency_rank = 1
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT agent_id, channel, days_since_closed_at
    FROM unique_rows
) src
PIVOT (
    SUM(days_since_closed_at)
    FOR channel IN ('chat', 'phone')
)
ORDER BY agent_id
