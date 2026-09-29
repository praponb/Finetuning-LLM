WITH prepared AS (
    -- Scope the tickets rows
    SELECT *
    FROM tickets
    WHERE opened_at < TIMESTAMP '2025-02-18 07:00:00'
      AND requester_id BETWEEN 7 AND 25
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        requester_id,
        closed_at,
        priority,
        channel,
        tags,
        LENGTH(TRIM(channel)) AS channel_len,
        opened_at + INTERVAL 2 HOURS AS opened_at_plus_2h
    FROM prepared
    WHERE closed_at >= TIMESTAMP '2025-02-02 03:00:00'
),
with_counts AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.id) AS last_event_at
    FROM scored s
),
without_matches AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM with_counts s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.id
              AND t.actor IN ('customer', 'system')
        )
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric_name, amount
FROM (
    SELECT id, channel_len, requester_id
    FROM without_matches
) src
UNPIVOT (
    amount FOR metric_name IN (channel_len AS channel_len_v, requester_id AS requester_id_v)
) unp
ORDER BY id, metric_name
