WITH valid_tickets AS (
    -- Keep valid rows and derive helper columns
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
    WHERE MONTH(opened_at) IN (1, 3)
),
unique_rows AS (
    -- Keep the earliest row per agent_id, channel
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
            ROW_NUMBER() OVER (PARTITION BY agent_id, channel ORDER BY closed_at, id) AS row_num
        FROM valid_tickets
    ) ranked
    WHERE row_num = 1
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        channel,
        DATE_TRUNC('MONTH', opened_at) AS opened_at_month,
        COUNT(*) AS row_count,
        COUNT(DISTINCT id) AS distinct_ids,
        ROUND(SUM(csat) / NULLIF(SUM(requester_id), 0), 3) AS csat_to_requester_id_ratio,
        MAX(csat) AS max_csat
    FROM unique_rows
    GROUP BY channel, DATE_TRUNC('MONTH', opened_at)
),
filtered AS (
    -- Compare against the overall max
    SELECT *
    FROM summary
    WHERE max_csat < (SELECT MAX(max_csat) FROM summary)
)
-- Native SQL UNPIVOT (wide to long)
SELECT channel, opened_at_month, measure, reading
FROM (
    SELECT channel, opened_at_month, row_count, distinct_ids, max_csat
    FROM filtered
) src
UNPIVOT INCLUDE NULLS (
    reading FOR measure IN (row_count, distinct_ids, max_csat)
) unp
ORDER BY channel, opened_at_month, measure
