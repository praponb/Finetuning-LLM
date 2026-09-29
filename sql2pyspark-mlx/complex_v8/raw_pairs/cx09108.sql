WITH prepared_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        channel,
        tags,
        TO_DATE(opened_at) AS opened_at_date,
        CONCAT(priority, '#', CAST(agent_id AS STRING)) AS priority_ref
    FROM tickets
    WHERE priority IN ('P3', 'P1', 'P4')
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        MIN(event_at) AS first_event_at,
        MIN(event_at) AS first_event_at_2,
        MIN(event_at) AS first_event_at_3
    FROM ticket_events
    GROUP BY ticket_id
),
with_stats AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.first_event_at,
        t.first_event_at_2,
        t.first_event_at_3
    FROM prepared_tickets s
    LEFT JOIN event_stats t ON t.ticket_id = s.id
),
with_children_events AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.opened_at_date,
        s.first_event_at,
        s.first_event_at_3,
        t.id AS event_id,
        t.event_at,
        t.event_type
    FROM with_stats s
    JOIN ticket_events t ON t.ticket_id = s.id AND t.event_type IN ('reopened', 'resolved', 'escalated')
)
-- Subtotals with GROUPING SETS
SELECT
    event_type,
    agent_id,
    event_id,
    COUNT(*) FILTER (WHERE event_type <> 'reopened') AS n_event_type_reopened,
    ROUND(100 * COUNT(CASE WHEN event_type <> 'replied' THEN 1 END) / COUNT(*), 1) AS pct_event_type_replied,
    COUNT(*) AS cnt
FROM with_children_events
GROUP BY GROUPING SETS ((event_type, agent_id, event_id), (event_type), (agent_id), ())
ORDER BY event_type NULLS LAST, agent_id NULLS LAST, event_id NULLS LAST
