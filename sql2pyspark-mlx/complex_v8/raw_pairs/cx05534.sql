WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        priority,
        channel,
        csat,
        tags,
        ABS(requester_id - 18) AS requester_id_distance
    FROM tickets
    WHERE csat < 5
),
with_counts AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.id) AS last_event_at
    FROM valid s
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, reading
FROM (
    SELECT id, csat, requester_id_distance
    FROM with_counts
) src
UNPIVOT (
    reading FOR field IN (csat AS csat_v, requester_id_distance AS m_requester_id_distance)
) unp
ORDER BY id, field
