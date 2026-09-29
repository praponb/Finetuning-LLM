WITH in_window AS (
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
with_counts AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.id) AS event_count
    FROM in_window s
)
SELECT
    id,
    agent_id,
    opened_at,
    prior_rows_3d
FROM with_counts
ORDER BY agent_id DESC, opened_at DESC
