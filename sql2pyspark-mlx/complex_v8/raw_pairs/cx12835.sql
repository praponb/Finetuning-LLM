WITH filtered_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_TRUNC('HOUR', event_at) AS event_at_hour,
        TO_DATE(event_at) AS event_at_date
    FROM ticket_events
    WHERE TO_DATE(event_at) < DATE '2025-02-06'
),
combined_details_ticket AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.event_at_hour,
        s.event_at_date,
        t.requester_id AS ticket_requester_id,
        t.csat AS ticket_csat,
        t.opened_at AS ticket_opened_at,
        su.tier AS agent_tier
    FROM filtered_events s
    JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
screened AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM combined_details_ticket s
    WHERE s.ticket_id IN (
            SELECT t.ticket_id
            FROM ticket_events t
            WHERE t.event_type IN ('assigned', 'reopened')
        )
      AND s.event_type <> 'reopened'
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        MAX(event_at) AS last_event_at,
        MIN(event_at) AS first_event_at,
        COUNT(*) AS event_count
    FROM ticket_events
    GROUP BY ticket_id
),
with_stats AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.last_event_at,
        t.first_event_at,
        COALESCE(t.event_count, 0) AS event_count
    FROM screened s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
)
SELECT
    ticket_csat,
    COALESCE(event_type, 'ALL') AS event_type,
    COUNT(*) AS n_rows
FROM with_stats
GROUP BY CUBE(ticket_csat, event_type)
ORDER BY ticket_csat NULLS LAST, event_type NULLS LAST
