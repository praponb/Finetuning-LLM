WITH filtered_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        priority,
        csat,
        tags
    FROM tickets
    WHERE closed_at >= TIMESTAMP '2025-01-14 03:00:00'
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY agent_id, csat) AS rows_in_agent_id_csat,
        MAX(requester_id) OVER w_agent_id_csat AS agent_id_csat_max_requester_id,
        LEAD(requester_id) OVER w_csat_by_requester_id AS next_requester_id,
        SUM(csat) OVER (PARTITION BY csat ORDER BY requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_csat
    FROM filtered_tickets
    WINDOW w_agent_id_csat AS (PARTITION BY agent_id, csat), w_csat_by_requester_id AS (PARTITION BY csat ORDER BY requester_id, id)
),
detail_rows AS (
    -- One row per event
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.tags,
        s.rows_in_agent_id_csat,
        s.next_requester_id,
        t.id AS event_id,
        t.event_type,
        t.event_at
    FROM with_trends s
    JOIN ticket_events t ON t.ticket_id = s.id AND t.event_at >= TIMESTAMP '2025-01-25 10:00:00'
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT event_id, agent_id, event_type, next_requester_id
    FROM detail_rows
) src
PIVOT (
    SUM(next_requester_id)
    FOR event_type IN ('replied', 'resolved', 'reopened', 'escalated')
)
ORDER BY event_id, agent_id
