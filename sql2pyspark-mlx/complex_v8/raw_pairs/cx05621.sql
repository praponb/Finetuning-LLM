WITH valid_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        tags,
        CONCAT(priority, '#', CAST(id AS STRING)) AS priority_ref
    FROM tickets
    WHERE csat > 3
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        MIN(event_at) AS first_event_at,
        MAX(event_at) AS last_event_at
    FROM ticket_events
    GROUP BY ticket_id
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.first_event_at,
        t.last_event_at
    FROM valid_tickets s
    LEFT JOIN event_stats t ON t.ticket_id = s.id
),
line_level_events AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.first_event_at,
        s.last_event_at,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_type
    FROM summarized_rows s
    JOIN ticket_events t ON t.ticket_id = s.id
),
enriched_event AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.first_event_at,
        s.event_id,
        s.event_actor,
        s.event_type,
        t.event_type AS event_event_type,
        t.event_at,
        ti.priority AS ticket_priority
    FROM line_level_events s
    JOIN ticket_events t ON t.id = s.event_id AND t.actor = 'agent'
    LEFT JOIN tickets ti ON ti.id = t.ticket_id
),
screened AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM enriched_event s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND t.requester_id < 17
              AND t.priority IN ('P1', 'P2')
        )
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN ticket_priority IN ('P4', 'P1') THEN 'group_a' WHEN ticket_priority = 'P2' THEN 'group_b' ELSE 'other' END AS ticket_priority_group,
        UPPER(TRIM(ticket_priority)) AS ticket_priority_clean
    FROM screened
    WHERE ticket_priority IS NOT NULL
)
-- Final projection
SELECT
    id,
    opened_at,
    closed_at,
    first_event_at,
    event_id,
    event_actor,
    event_type,
    event_at,
    ticket_priority_group,
    ticket_priority_clean,
    INITCAP(TRIM(ticket_priority_group)) AS ticket_priority_group_clean
FROM derived
ORDER BY opened_at
