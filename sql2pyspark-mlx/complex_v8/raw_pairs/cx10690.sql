WITH recent AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        tags,
        CASE WHEN priority IN ('P2', 'P3') THEN 'group_a' WHEN priority = 'P1' THEN 'group_b' ELSE 'other' END AS priority_group,
        DATE_TRUNC('DAY', closed_at) AS closed_at_day,
        ROUND(csat / NULLIF(requester_id, 0), 3) AS csat_per_requester_id
    FROM tickets
    WHERE channel = 'phone'
      AND NOT ARRAY_CONTAINS(tags, 'outage')
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM recent
    WHERE csat_per_requester_id > (SELECT AVG(csat_per_requester_id) FROM recent)
),
profiled AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MIN(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.id) AS first_event_at,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.id) AS event_count
    FROM without_matches s
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric, reading
FROM (
    SELECT id, event_count, requester_id
    FROM profiled
) src
UNPIVOT (
    reading FOR metric IN (event_count, requester_id)
) unp
