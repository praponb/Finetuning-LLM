WITH recent_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        tags
    FROM tickets
    WHERE requester_id <= 14
),
detail_rows AS (
    -- One row per event
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.priority,
        s.tags,
        t.id AS event_id,
        t.event_at,
        t.event_type
    FROM recent_tickets s
    JOIN ticket_events t ON t.ticket_id = s.id
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        requester_id,
        agent_id,
        tags,
        event_id,
        event_at,
        MIN(requester_id) OVER (PARTITION BY priority, YEAR(event_at)) AS priority_event_at_year_min_requester_id,
        ROW_NUMBER() OVER (PARTITION BY agent_id ORDER BY requester_id, id, event_id) AS seq_no,
        LEAD(requester_id) OVER (PARTITION BY TO_DATE(event_at) ORDER BY requester_id ASC, id, event_id) AS next_requester_id
    FROM detail_rows
),
with_children_events AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.tags,
        s.event_id,
        s.seq_no,
        s.next_requester_id,
        t.id AS event_id_2,
        t.event_type,
        t.event_at AS event_event_at
    FROM with_windows s
    JOIN ticket_events t ON t.ticket_id = s.id
)
-- Subtotals with ROLLUP
SELECT
    event_id_2,
    event_id,
    COALESCE(event_type, 'ALL') AS event_type,
    COUNT(DISTINCT event_id_2) AS distinct_event_id_2s,
    COUNT(*) AS n_rows
FROM with_children_events
GROUP BY ROLLUP(event_id_2, event_id, event_type)
ORDER BY event_id_2 NULLS LAST, event_id NULLS LAST, event_type NULLS LAST
