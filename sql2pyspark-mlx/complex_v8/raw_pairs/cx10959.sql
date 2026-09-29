WITH scoped AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        csat,
        tags,
        CONCAT(priority, '#', CAST(requester_id AS STRING)) AS priority_ref,
        CASE WHEN csat >= 4 THEN 'A' WHEN csat >= 2 THEN 'B' ELSE 'C' END AS csat_tier,
        closed_at + INTERVAL 30 MINUTES AS closed_at_plus_30m
    FROM tickets
    WHERE csat BETWEEN 2 AND 5
      AND priority <> 'P4'
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY priority ORDER BY opened_at, id) AS opened_at_quintile,
        LEAD(closed_at) OVER (PARTITION BY priority ORDER BY opened_at, id) AS next_closed_at,
        COUNT(*) OVER w_closed_at_plus_30m_month AS rows_in_closed_at_plus_30m_month
    FROM scoped
    WINDOW w_closed_at_plus_30m_month AS (PARTITION BY MONTH(closed_at_plus_30m))
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, amount
FROM (
    SELECT id, opened_at_quintile, csat, rows_in_closed_at_plus_30m_month
    FROM metrics
) src
UNPIVOT (
    amount FOR measure IN (opened_at_quintile, csat, rows_in_closed_at_plus_30m_month)
) unp
