WITH base AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        FLOOR(requester_id / 5) * 5 AS requester_id_bucket
    FROM tickets
    WHERE (opened_at >= TIMESTAMP '2025-02-06 11:00:00' OR csat < 4)
),
with_activity AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM base s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.id
              AND HOUR(t.event_at) BETWEEN 9 AND 15
              AND t.event_type IN ('replied', 'reopened', 'resolved')
        )
      AND s.agent_id >= 4
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        requester_id_bucket,
        ABS(requester_id_bucket - 15) AS requester_id_bucket_distance,
        FLOOR(requester_id / 5) * 5 AS requester_id_bucket_2,
        requester_id_bucket + requester_id AS requester_id_bucket_plus_requester_id
    FROM with_activity
    WHERE closed_at >= TIMESTAMP '2025-02-08 15:00:00'
)
SELECT
    id,
    requester_id,
    agent_id,
    opened_at,
    closed_at,
    requester_id_bucket,
    requester_id_bucket_distance,
    requester_id_bucket_2,
    requester_id_bucket_plus_requester_id
FROM scored
WHERE requester_id_bucket_plus_requester_id <= 30
ORDER BY id
