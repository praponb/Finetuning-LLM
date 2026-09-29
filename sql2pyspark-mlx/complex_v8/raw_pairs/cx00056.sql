WITH base_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        csat,
        tags,
        LOWER(TRIM(priority)) AS priority_clean
    FROM tickets
    WHERE priority IS NOT NULL
),
qualified AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM base_tickets s
    WHERE s.agent_id IN (
            SELECT t.agent_id
            FROM tickets t
            WHERE t.opened_at >= TIMESTAMP '2025-01-31 06:00:00'
              AND t.priority = 'P3'
        )
)
SELECT
    id,
    agent_id,
    closed_at,
    csat,
    tags,
    priority_clean,
    LOWER(REGEXP_REPLACE(TRIM(priority_clean), ' ', '_')) AS priority_clean_slug,
    CASE WHEN csat >= 4 THEN 'large' WHEN csat >= 3 THEN 'medium' ELSE 'small' END AS csat_tier
FROM qualified
ORDER BY agent_id, closed_at DESC NULLS FIRST
