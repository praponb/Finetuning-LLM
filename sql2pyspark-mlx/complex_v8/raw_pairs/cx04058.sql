WITH recent_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATEDIFF(DATE '2025-06-30', event_at) AS days_since_event_at
    FROM ticket_events
    WHERE HOUR(event_at) BETWEEN 7 AND 11
      AND actor = 'system'
),
combined_details AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.days_since_event_at,
        t.csat AS ticket_csat,
        su.agent_name
    FROM recent_events s
    JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
    WHERE t.csat < 4
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        DATEDIFF(DATE '2025-06-30', event_at) AS days_since_event_at_2,
        DATE_TRUNC('MONTH', event_at) AS event_at_month,
        event_at + INTERVAL 30 MINUTES AS event_at_plus_30m
    FROM combined_details
    WHERE ticket_id <= 48
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT ticket_id, MAX(event_at) AS last_event_at
    FROM ticket_events
    GROUP BY ticket_id
),
with_counts AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.last_event_at
    FROM scored s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    actor,
    days_since_event_at,
    ticket_csat,
    days_since_event_at_2,
    event_at_month,
    event_at_plus_30m,
    last_event_at,
    CONCAT(event_type, '#', CAST(ticket_csat AS STRING)) AS event_type_ref
FROM with_counts
ORDER BY days_since_event_at_2
