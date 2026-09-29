WITH scoped_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LOWER(REGEXP_REPLACE(TRIM(event_type), ' ', '_')) AS event_type_slug
    FROM ticket_events
    WHERE (ticket_id >= 50 OR event_type IS NOT NULL)
),
detail_rows_events AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        s.event_type_slug,
        t.id AS event_id,
        t.event_at AS event_event_at,
        t.event_type AS event_event_type
    FROM scoped_events s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id AND HOUR(t.event_at) BETWEEN 7 AND 9
),
combined_details_event AS (
    -- Attach event attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        s.event_type_slug,
        s.event_id,
        s.event_event_at,
        s.event_event_type,
        t.ticket_id AS event_ticket_id,
        ti.opened_at AS ticket_opened_at,
        ti.requester_id AS ticket_requester_id
    FROM detail_rows_events s
    JOIN ticket_events t ON t.id = s.event_id
    LEFT JOIN tickets ti ON ti.id = s.ticket_id
),
detail_rows_events_2 AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_type_slug,
        s.event_id,
        s.event_event_at,
        s.event_ticket_id,
        s.ticket_opened_at,
        s.ticket_requester_id,
        t.id AS event_id_2,
        t.event_at,
        t.actor AS event_actor,
        t.event_type AS event_event_type_2
    FROM combined_details_event s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id
),
rollup_base AS (
    -- Summarise per group
    SELECT
        ticket_id,
        YEAR(ticket_opened_at) AS ticket_opened_at_year,
        SUM(CASE WHEN event_event_type_2 = 'escalated' THEN ticket_requester_id ELSE 0 END) AS ticket_requester_id_event_event_type_2_escalated,
        COUNT(*) AS row_count,
        ROUND(STDDEV(ticket_requester_id), 2) AS stddev_ticket_requester_id,
        MAX(event_at) AS last_event_at
    FROM detail_rows_events_2
    GROUP BY ticket_id, YEAR(ticket_opened_at)
    HAVING COUNT(*) >= 2
)
SELECT
    ticket_id,
    ticket_requester_id_event_event_type_2_escalated,
    stddev_ticket_requester_id,
    last_event_at,
    DAYOFWEEK(last_event_at) AS last_event_at_dayofweek,
    ROUND(ticket_opened_at_year / NULLIF(row_count, 0), 3) AS ticket_opened_at_year_per_row_count
FROM rollup_base
ORDER BY ticket_opened_at_year_per_row_count NULLS LAST, stddev_ticket_requester_id NULLS LAST
