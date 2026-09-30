WITH scoped_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_TRUNC('HOUR', event_at) AS event_at_hour,
        DATE_TRUNC('QUARTER', event_at) AS event_at_quarter
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS last_event_at,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_count
    FROM scoped_events s
),
event_stats_2 AS (
    -- Per-ticket ticket_events metrics
    SELECT ticket_id, COUNT(*) AS event_count_2, MIN(event_at) AS first_event_at
    FROM ticket_events
    GROUP BY ticket_id
),
with_counts AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        COALESCE(t.event_count_2, 0) AS event_count_2,
        t.first_event_at
    FROM summarized_rows s
    LEFT JOIN event_stats_2 t ON t.ticket_id = s.ticket_id
)
-- Subtotals with GROUPING SETS
SELECT
    ticket_id,
    COALESCE(event_type, 'ALL') AS event_type,
    event_count_2,
    GROUPING(ticket_id) AS is_total_ticket_id,
    ROUND(SUM(event_count) / NULLIF(SUM(event_count_2), 0), 3) AS event_count_to_event_count_2_ratio,
    COUNT(*) AS cnt
FROM with_counts
GROUP BY GROUPING SETS ((ticket_id, event_type, event_count_2), (ticket_id), (event_type), ())
ORDER BY ticket_id NULLS LAST, event_type NULLS LAST, event_count_2 NULLS LAST
