WITH valid_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        priority,
        channel,
        tags,
        COALESCE(requester_id, 0) AS requester_id_filled,
        ABS(csat - 3) AS csat_distance
    FROM tickets
    WHERE requester_id > 9
),
with_activity AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM valid_tickets s
    WHERE s.id IN (
            SELECT t.ticket_id
            FROM ticket_events t
            WHERE t.actor NOT IN ('customer', 'agent')
        )
),
qualified AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM with_activity s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.id
              AND t.event_type IN ('reopened', 'assigned', 'escalated')
              AND t.event_at < TIMESTAMP '2025-03-02 09:00:00'
        )
)
-- Subtotals with GROUPING SETS
SELECT
    channel,
    priority,
    ARRAY_SORT(COLLECT_SET(priority)) AS priority_list,
    MIN(csat_distance) AS min_csat_distance,
    SUM(requester_id) AS total_requester_id
FROM qualified
GROUP BY GROUPING SETS ((channel, priority), (channel), ())
