WITH joined_ranges AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.agent_id,
        a.opened_at,
        COUNT(b.id) AS prior_rows_1d,
        SUM(b.requester_id) AS prior_requester_id_1d
    FROM tickets a
    LEFT JOIN tickets b
      ON b.agent_id = a.agent_id
     AND b.opened_at < a.opened_at
     AND b.opened_at >= a.opened_at - INTERVAL 1 DAYS
    GROUP BY a.id, a.agent_id, a.opened_at
),
with_children_events AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.prior_rows_1d,
        s.prior_requester_id_1d,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_type
    FROM joined_ranges s
    LEFT JOIN ticket_events t ON t.ticket_id = s.id
),
screened AS (
    -- Rows with no matching tickets
    SELECT s.*
    FROM with_children_events s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND t.priority IN ('P2', 'P1', 'P3')
        )
      AND s.event_id <= 28
)
-- Final projection
SELECT
    id,
    opened_at,
    prior_rows_1d,
    prior_requester_id_1d,
    event_id,
    event_actor,
    INITCAP(TRIM(event_actor)) AS event_actor_clean
FROM screened
ORDER BY event_id, id
