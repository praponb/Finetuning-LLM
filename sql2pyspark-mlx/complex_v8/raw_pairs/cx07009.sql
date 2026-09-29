WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_FORMAT(event_at, 'yyyy-MM') AS event_at_label,
        DATEDIFF(DATE '2025-12-31', event_at) AS days_since_event_at,
        LOWER(TRIM(actor)) AS actor_clean
    FROM ticket_events
    WHERE actor = 'system'
),
combined_details_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.event_at_label,
        s.actor_clean,
        t.channel AS ticket_channel,
        t.tags AS ticket_tags,
        t.priority AS ticket_priority
    FROM scoped s
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_all_by_event_at, 3) AS event_at_percent_rank,
        FIRST_VALUE(event_at) OVER (ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        LEAD(event_at) OVER (PARTITION BY ticket_channel ORDER BY event_at DESC, id) AS next_event_at
    FROM combined_details_ticket
    WINDOW w_all_by_event_at AS (ORDER BY event_at DESC, id)
),
line_level AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.actor,
        s.ticket_channel,
        s.ticket_tags,
        s.event_at_percent_rank,
        s.first_event_at,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_at AS event_event_at,
        t.event_type AS event_event_type
    FROM with_windows s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id
)
-- Subtotals with CUBE
SELECT
    COALESCE(event_actor, 'ALL') AS event_actor,
    COALESCE(ticket_channel, 'ALL') AS ticket_channel,
    COUNT(*) AS cnt,
    COUNT(DISTINCT event_id) AS distinct_event_ids
FROM line_level
GROUP BY CUBE(event_actor, ticket_channel)
