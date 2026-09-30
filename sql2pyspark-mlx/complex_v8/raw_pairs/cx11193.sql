WITH valid AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATEDIFF(DATE '2025-12-31', event_at) AS days_since_event_at,
        DATE_FORMAT(event_at, 'yyyy-MM') AS event_at_label,
        CASE WHEN event_type IN ('reopened', 'replied') THEN 'group_a' WHEN event_type = 'escalated' THEN 'group_b' ELSE 'other' END AS event_type_group
    FROM ticket_events
    WHERE actor NOT IN ('system', 'agent')
      AND TO_DATE(event_at) < DATE '2025-03-02'
),
detail_rows_events AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.event_at_label,
        s.event_type_group,
        t.id AS event_id,
        t.event_type AS event_event_type,
        t.actor AS event_actor,
        t.event_at AS event_event_at
    FROM valid s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        ticket_id,
        event_type,
        event_type_group,
        event_id,
        event_actor,
        event_event_at,
        RANK() OVER (PARTITION BY event_id ORDER BY event_at DESC) AS event_at_rank
    FROM detail_rows_events
)
-- Subtotals with GROUPING SETS
SELECT
    ticket_id,
    event_id,
    COUNT(DISTINCT ticket_id) AS distinct_ticket_ids
FROM windowed
GROUP BY GROUPING SETS ((ticket_id, event_id), (ticket_id))
ORDER BY ticket_id NULLS LAST, event_id NULLS LAST
