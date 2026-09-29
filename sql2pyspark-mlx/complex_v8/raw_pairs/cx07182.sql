WITH cleaned AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        TO_DATE(event_at) AS event_at_date
    FROM ticket_events
    WHERE event_at < TIMESTAMP '2025-03-08 08:00:00'
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        COUNT(*) FILTER (WHERE event_type = 'assigned') AS event_event_type_count
    FROM ticket_events
    GROUP BY ticket_id
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        COALESCE(t.event_event_type_count, 0) AS event_event_type_count
    FROM cleaned s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        event_type,
        actor,
        event_at_date,
        event_event_type_count,
        NTH_VALUE(event_at_date, 2) OVER (PARTITION BY ticket_id, event_type ORDER BY event_at_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at_date,
        NTH_VALUE(event_at_date, 2) OVER (PARTITION BY ticket_id ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at_date_2,
        SUM(event_event_type_count) OVER (PARTITION BY ticket_id ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_event_event_type_count,
        COUNT(*) OVER (PARTITION BY event_event_type_count, MONTH(event_at)) AS rows_in_event_event_type_count_event_at_month
    FROM summarized_rows
),
without_matches AS (
    -- Compare against the overall min
    SELECT *
    FROM with_trends
    WHERE running_event_event_type_count > (SELECT MIN(running_event_event_type_count) FROM with_trends)
)
SELECT *
FROM (
    SELECT second_event_at_date, event_event_type_count, actor, running_event_event_type_count
    FROM without_matches
) src
PIVOT (
    SUM(running_event_event_type_count)
    FOR actor IN ('customer', 'agent')
)
ORDER BY second_event_at_date, event_event_type_count
