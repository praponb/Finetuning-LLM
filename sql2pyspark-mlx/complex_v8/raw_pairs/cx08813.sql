WITH matched AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.agent_id,
        a.opened_at,
        COUNT(b.id) AS prior_rows_3d,
        SUM(b.csat) AS prior_csat_3d
    FROM tickets a
    LEFT JOIN tickets b
      ON b.agent_id = a.agent_id
     AND b.opened_at < a.opened_at
     AND b.opened_at >= a.opened_at - INTERVAL 3 DAYS
    GROUP BY a.id, a.agent_id, a.opened_at
),
qualified AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM matched s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.id
        )
),
with_activity AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM qualified s
    WHERE s.id IN (
            SELECT t.ticket_id
            FROM ticket_events t
        )
)
SELECT
    prior_rows_3d,
    agent_id,
    GROUPING_ID() AS grouping_level,
    MIN(opened_at) AS first_opened_at,
    COUNT(*) AS cnt
FROM with_activity
GROUP BY ROLLUP(prior_rows_3d, agent_id)
