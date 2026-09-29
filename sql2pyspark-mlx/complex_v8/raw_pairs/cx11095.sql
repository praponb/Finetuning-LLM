WITH recent_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_at + INTERVAL 1 DAYS AS event_at_plus_1d,
        DATE_FORMAT(event_at, 'MMM yyyy') AS event_at_label
    FROM ticket_events
    WHERE actor = 'customer'
),
profiled AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS last_event_at,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_count
    FROM recent_events s
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN t.event_type IN ('escalated', 'reopened') THEN 1 END) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_event_type_count,
        (SELECT MIN(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS first_event_at
    FROM profiled s
),
rollup_base AS (
    -- Summarise per group
    SELECT
        actor,
        ticket_id,
        MAX(event_at_plus_1d) AS last_event_at_plus_1d,
        ARRAY_SORT(COLLECT_SET(event_type)) AS event_type_list
    FROM summarized_rows
    GROUP BY 1, 2
    HAVING MAX(event_event_type_count) > 3
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        DATEDIFF(DATE '2025-12-31', last_event_at_plus_1d) AS days_since_last_event_at_plus_1d
    FROM rollup_base
    WHERE actor IN ('agent', 'system')
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.actor,
        s.ticket_id,
        s.last_event_at_plus_1d,
        s.event_type_list,
        s.days_since_last_event_at_plus_1d,
        t.closed_at AS ticket_closed_at
    FROM tagged s
    JOIN tickets t ON t.id = s.ticket_id
    WHERE t.closed_at < TIMESTAMP '2025-02-15 09:00:00'
)
-- Final projection
SELECT
    actor,
    ticket_id,
    days_since_last_event_at_plus_1d,
    ticket_closed_at,
    DATE_FORMAT(ticket_closed_at, 'yyyy-MM-dd HH:00') AS ticket_closed_at_label
FROM enriched
WHERE ticket_id <= 38
