WITH prepared_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT(event_type, '#', CAST(ticket_id AS STRING)) AS event_type_ref
    FROM ticket_events
    WHERE event_at >= TIMESTAMP '2025-01-31 08:00:00'
),
tagged AS (
    -- Add calculated columns
    SELECT *, QUARTER(event_at) AS event_at_quarter
    FROM prepared_events
    WHERE event_type_ref IS NOT NULL
),
detail_rows_events AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.actor,
        s.event_at_quarter,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_type AS event_event_type,
        t.event_at AS event_event_at
    FROM tagged s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS last_event_at,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS last_event_at_2
    FROM detail_rows_events s
),
unique_rows AS (
    -- Keep the latest row per ticket_id
    SELECT
        id,
        ticket_id,
        actor,
        event_at_quarter,
        event_id,
        event_actor,
        event_event_type,
        event_event_at,
        last_event_at,
        last_event_at_2
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY ticket_id ORDER BY last_event_at_2 DESC, id DESC, event_id DESC) AS row_num
        FROM summarized_rows
    ) ranked
    WHERE row_num = 1
)
SELECT
    id,
    ticket_id,
    event_at_quarter,
    event_id,
    event_actor,
    event_event_type,
    event_event_at,
    last_event_at,
    last_event_at_2,
    COALESCE(event_at_quarter, 0) AS event_at_quarter_filled
FROM unique_rows
ORDER BY event_id
