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
with_activity AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM covered s
    WHERE s.agent_id IN (
            SELECT t.agent_id
            FROM tickets t
        )
),
annotated AS (
    -- Label and score rows
    SELECT *, DATE_FORMAT(opened_at, 'MMM yyyy') AS opened_at_label
    FROM with_activity
),
exploded_detail AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.prior_rows_7d,
        s.prior_requester_id_7d,
        s.opened_at_label,
        t.id AS event_id,
        t.event_type,
        t.event_at,
        t.actor AS event_actor
    FROM annotated s
    LEFT JOIN ticket_events t ON t.ticket_id = s.id
)
SELECT
    id,
    opened_at,
    prior_rows_7d,
    opened_at_label,
    event_id,
    event_type,
    event_at,
    event_actor,
    ABS(prior_rows_7d - 4) AS prior_rows_7d_distance,
    LOWER(TRIM(opened_at_label)) AS opened_at_label_clean
FROM exploded_detail
