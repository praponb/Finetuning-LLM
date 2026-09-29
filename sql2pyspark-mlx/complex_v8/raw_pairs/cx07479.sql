WITH covered AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.agent_id,
        a.opened_at,
        COUNT(b.id) AS prior_rows_12h,
        SUM(b.csat) AS prior_csat_12h
    FROM tickets a
    LEFT JOIN tickets b
      ON b.agent_id = a.agent_id
     AND b.opened_at < a.opened_at
     AND b.opened_at >= a.opened_at - INTERVAL 12 HOURS
    GROUP BY a.id, a.agent_id, a.opened_at
),
with_activity AS (
    -- Keep rows whose agent matches
    SELECT s.*
    FROM covered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM support_agents su
            WHERE su.id = s.agent_id
              AND UPPER(TRIM(su.agent_name)) LIKE 'F%'
        )
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric, value
FROM (
    SELECT id, prior_rows_12h, prior_csat_12h
    FROM with_activity
) src
UNPIVOT INCLUDE NULLS (
    value FOR metric IN (prior_rows_12h, prior_csat_12h)
) unp
ORDER BY id, metric
