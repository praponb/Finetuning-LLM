WITH scoped_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        csat,
        tags
    FROM tickets
    WHERE opened_at >= TIMESTAMP '2025-01-19 10:00:00'
      AND priority NOT IN ('P3', 'P1')
),
deduped AS (
    -- Keep the earliest row per agent_id, priority
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        csat,
        tags
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY agent_id, priority ORDER BY closed_at, id) AS recency_rank
        FROM scoped_tickets
    ) ranked
    WHERE recency_rank = 1
)
-- Subtotals with GROUPING SETS
SELECT
    csat,
    COALESCE(priority, 'ALL') AS priority,
    GROUPING_ID() AS gid,
    MAX(closed_at) AS last_closed_at
FROM deduped
GROUP BY GROUPING SETS ((csat, priority), (csat), (priority), ())
ORDER BY csat NULLS LAST, priority NULLS LAST
