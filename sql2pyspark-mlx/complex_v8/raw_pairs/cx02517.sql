WITH cleaned_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT(actor, '#', CAST(ticket_id AS STRING)) AS actor_ref
    FROM ticket_events
    WHERE event_type IN ('escalated', 'assigned')
      AND ticket_id IS NOT NULL
),
combined_details_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.actor_ref,
        t.channel AS ticket_channel,
        t.tags AS ticket_tags,
        t.closed_at AS ticket_closed_at
    FROM cleaned_events s
    JOIN tickets t ON t.id = s.ticket_id
    WHERE t.closed_at < TIMESTAMP '2025-03-12 01:00:00'
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LEAD(ticket_closed_at) OVER (ORDER BY ticket_closed_at, id) AS next_ticket_closed_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY event_type ORDER BY ticket_closed_at), 3) AS ticket_closed_at_percent_rank,
        FIRST_VALUE(event_at) OVER (PARTITION BY event_type ORDER BY ticket_closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        RANK() OVER (PARTITION BY event_type ORDER BY ticket_closed_at) AS ticket_closed_at_rank,
        LAST_VALUE(event_at) OVER (ORDER BY event_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at,
        LEAD(ticket_closed_at, 2) OVER (ORDER BY ticket_closed_at, id) AS next2_ticket_closed_at,
        NTILE(3) OVER (ORDER BY event_at NULLS LAST, id) AS event_at_tercile,
        ROUND(PERCENT_RANK() OVER (ORDER BY event_at NULLS LAST), 3) AS event_at_percent_rank
    FROM combined_details_ticket
),
leaders AS (
    -- Top 3 per group
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        actor_ref,
        ticket_channel,
        ticket_tags,
        ticket_closed_at,
        next_ticket_closed_at,
        ticket_closed_at_percent_rank,
        first_event_at,
        last_event_at,
        next2_ticket_closed_at,
        event_at_tercile,
        event_at_percent_rank
    FROM with_windows
    WHERE ticket_closed_at_rank <= 3
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    ticket_channel,
    ticket_closed_at,
    next_ticket_closed_at,
    ticket_closed_at_percent_rank,
    next2_ticket_closed_at,
    event_at_tercile,
    event_at_percent_rank,
    DATE_TRUNC('MONTH', next_ticket_closed_at) AS next_ticket_closed_at_month,
    COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown
FROM leaders
ORDER BY ticket_id, event_type DESC, id
LIMIT 20
