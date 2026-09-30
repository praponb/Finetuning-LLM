WITH cleaned_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
),
enriched AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        t.priority AS ticket_priority,
        t.agent_id AS ticket_agent_id
    FROM cleaned_events s
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
detail_rows AS (
    -- One row per ticket
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.ticket_priority,
        s.ticket_agent_id,
        t.id AS ticket_id_2,
        t.priority AS ticket_priority_2,
        t.closed_at AS ticket_closed_at,
        t.csat AS ticket_csat
    FROM enriched s
    JOIN tickets t ON t.agent_id = s.ticket_agent_id
),
enriched_2 AS (
    -- Add calculated columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        ticket_priority,
        ticket_agent_id,
        ticket_id_2,
        ticket_priority_2,
        ticket_csat,
        CASE WHEN ticket_csat >= 4 THEN 'HIGH' WHEN ticket_csat >= 2 THEN 'MEDIUM' ELSE 'LOW' END AS ticket_csat_tier,
        DATE_FORMAT(event_at, 'yyyy-MM-dd') AS event_at_label,
        DATEDIFF(DATE '2025-12-31', event_at) AS days_since_event_at
    FROM detail_rows
    WHERE ticket_priority_2 = 'P2'
)
-- Final projection
SELECT
    id,
    event_at,
    event_type,
    ticket_priority,
    ticket_agent_id,
    ticket_id_2,
    ticket_csat_tier,
    days_since_event_at,
    CASE WHEN ticket_priority IN ('P2', 'P3') THEN 'group_a' WHEN ticket_priority = 'P4' THEN 'group_b' ELSE 'other' END AS ticket_priority_group,
    DATE_TRUNC('YEAR', event_at) AS event_at_year
FROM enriched_2
ORDER BY id, ticket_priority_group DESC
