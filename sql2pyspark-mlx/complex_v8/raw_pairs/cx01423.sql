WITH prepared_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug,
        DATEDIFF(DATE '2026-01-01', event_at) AS days_since_event_at
    FROM ticket_events
    WHERE (event_type = 'assigned' OR ticket_id <= 48)
),
with_activity AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM prepared_events s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.actor IN ('system', 'agent')
              AND t.event_at < TIMESTAMP '2025-02-12 13:00:00'
        )
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        event_at,
        event_type,
        actor,
        actor_slug,
        days_since_event_at,
        ROUND(days_since_event_at / NULLIF(SUM(days_since_event_at) OVER w_event_at_month_ticket_id, 0), 4) AS days_since_event_at_share_of_event_at_month_tick,
        NTILE(10) OVER (ORDER BY event_at DESC, id) AS event_at_decile,
        ROW_NUMBER() OVER (ORDER BY event_at DESC, id) AS rn,
        MIN(days_since_event_at) OVER w_event_at_month_ticket_id AS event_at_month_ticket_id_min_days_since_event_at
    FROM with_activity
    WINDOW w_event_at_month_ticket_id AS (PARTITION BY MONTH(event_at), ticket_id)
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, reading
FROM (
    SELECT id, rn, event_at_month_ticket_id_min_days_since_event_at, days_since_event_at
    FROM windowed
) src
UNPIVOT (
    reading FOR measure IN (rn, event_at_month_ticket_id_min_days_since_event_at, days_since_event_at)
) unp
ORDER BY id, measure
