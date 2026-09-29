WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        opened_at,
        priority,
        csat
    FROM tickets
    WHERE ARRAY_CONTAINS(tags, 'outage')
      AND opened_at < TIMESTAMP '2025-02-06 15:00:00'
)
SELECT priority, opened_at
FROM recent
UNION
SELECT priority, closed_at AS opened_at
FROM tickets
WHERE agent_id IS NOT NULL
  AND csat >= 2
ORDER BY opened_at
