WITH covered AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.agent_id,
        a.opened_at,
        COUNT(b.id) AS prior_rows_7d,
        SUM(b.requester_id) AS prior_requester_id_7d
    FROM tickets a
    LEFT JOIN tickets b
      ON b.agent_id = a.agent_id
     AND b.opened_at < a.opened_at
     AND b.opened_at >= a.opened_at - INTERVAL 7 DAYS
    GROUP BY a.id, a.agent_id, a.opened_at
),
exploded_detail AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.prior_rows_7d,
        s.prior_requester_id_7d,
        t.id AS event_id,
        t.event_at,
        t.event_type,
        t.actor AS event_actor
    FROM covered s
    JOIN ticket_events t ON t.ticket_id = s.id
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, event_id, measure, metric_value
FROM (
    SELECT id, event_id, prior_rows_7d, prior_requester_id_7d
    FROM exploded_detail
) src
UNPIVOT (
    metric_value FOR measure IN (prior_rows_7d, prior_requester_id_7d)
) unp
ORDER BY id, event_id, measure
