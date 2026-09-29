WITH filtered_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        csat
    FROM tickets
    WHERE HOUR(closed_at) BETWEEN 9 AND 14
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        requester_id,
        opened_at,
        ROUND(csat / NULLIF(requester_id, 0), 3) AS csat_per_requester_id,
        COALESCE(csat, 0) AS csat_filled
    FROM filtered_tickets
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        opened_at,
        csat_per_requester_id,
        csat_filled,
        ABS(csat_per_requester_id - 5.5) AS csat_per_requester_id_distance,
        DATE_TRUNC('QUARTER', opened_at) AS opened_at_quarter
    FROM derived
),
profiled AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.id) AS last_event_at,
        (SELECT MIN(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.id) AS first_event_at
    FROM tagged s
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, reading
FROM (
    SELECT id, csat_per_requester_id, csat_per_requester_id_distance
    FROM profiled
) src
UNPIVOT (
    reading FOR field IN (csat_per_requester_id, csat_per_requester_id_distance)
) unp
ORDER BY id, field
