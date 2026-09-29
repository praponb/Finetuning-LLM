WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT_WS('-', event_type, actor) AS event_type_actor_label,
        DATEDIFF(DATE '2026-01-01', event_at) AS days_since_event_at,
        DATEDIFF(DATE '2025-12-31', event_at) AS days_since_event_at_2
    FROM ticket_events
    WHERE ticket_id >= 46
      AND event_type <> 'resolved'
),
qualified AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND HOUR(t.event_at) BETWEEN 10 AND 15
              AND t.event_type NOT IN ('assigned', 'reopened')
        )
),
filtered AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM qualified s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.actor IN ('agent', 'system')
              AND HOUR(t.event_at) BETWEEN 7 AND 9
        )
),
with_details_ticket AS (
    -- Join tickets
    SELECT
        s.*,
        t.channel AS ticket_channel,
        t.opened_at AS ticket_opened_at,
        t.csat AS ticket_csat
    FROM filtered s
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        MAX(event_at) AS last_event_at,
        COUNT(*) FILTER (WHERE actor = 'agent') AS event_actor_count
    FROM ticket_events
    GROUP BY ticket_id
),
profiled AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.last_event_at,
        COALESCE(t.event_actor_count, 0) AS event_actor_count
    FROM with_details_ticket s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        event_type_actor_label,
        days_since_event_at_2,
        ticket_channel,
        ticket_opened_at,
        ticket_csat,
        GREATEST(event_actor_count, days_since_event_at) AS greatest_event_actor_count_days_since_event_at,
        LOWER(REGEXP_REPLACE(TRIM(ticket_channel), ' ', '_')) AS ticket_channel_slug,
        FLOOR(days_since_event_at / 100) * 100 AS days_since_event_at_bucket
    FROM profiled
    WHERE ticket_csat <= 4
)
SELECT
    id,
    ticket_id,
    event_type,
    event_type_actor_label,
    days_since_event_at_2,
    ticket_channel,
    ticket_opened_at,
    ticket_csat,
    greatest_event_actor_count_days_since_event_at,
    ticket_channel_slug,
    days_since_event_at_bucket,
    YEAR(ticket_opened_at) AS ticket_opened_at_year,
    CASE WHEN event_type IN ('created', 'assigned') THEN 'group_a' WHEN event_type = 'reopened' THEN 'group_b' ELSE 'other' END AS event_type_group
FROM scored
ORDER BY days_since_event_at_bucket DESC NULLS FIRST
