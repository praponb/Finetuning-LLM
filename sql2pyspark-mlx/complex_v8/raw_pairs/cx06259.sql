WITH cleaned_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        opened_at,
        priority,
        channel,
        csat,
        tags,
        CONCAT(priority, '#', CAST(agent_id AS STRING)) AS priority_ref,
        CONCAT(priority, '#', CAST(agent_id AS STRING)) AS priority_ref_2
    FROM tickets
    WHERE closed_at < TIMESTAMP '2025-02-26 16:00:00'
),
unique_rows AS (
    -- Keep the latest row per priority
    SELECT
        id,
        agent_id,
        opened_at,
        priority,
        channel,
        csat,
        tags,
        priority_ref,
        priority_ref_2
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY priority ORDER BY opened_at DESC, id DESC) AS row_num
        FROM cleaned_tickets
    ) ranked
    WHERE row_num = 1
),
totals AS (
    -- Summarise per group
    SELECT
        priority,
        csat,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(*) AS n_rows,
        SUM(csat) FILTER (WHERE priority = 'P4') AS csat_priority_p4,
        ARRAY_SORT(COLLECT_SET(channel)) AS channel_list,
        ROUND(AVG(csat), 2) AS avg_csat
    FROM unique_rows
    WHERE priority IS NOT NULL
    GROUP BY priority, csat
),
scored AS (
    -- Label and score rows
    SELECT *, LENGTH(TRIM(priority)) AS priority_len
    FROM totals
)
-- Final projection
SELECT
    priority,
    csat,
    n_rows,
    channel_list,
    avg_csat,
    LEAST(n_rows, csat_priority_p4) AS least_n_rows_csat_priority_p4
FROM scored
ORDER BY n_rows
