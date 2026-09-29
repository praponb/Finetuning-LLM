WITH recent_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_FORMAT(event_at, 'yyyy-MM') AS event_at_label,
        QUARTER(event_at) AS event_at_quarter
    FROM ticket_events
    WHERE event_type <> 'reopened'
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_count,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_count_2
    FROM recent_events s
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_ticket_id_by_event_count_tie, 3) AS event_count_percent_rank,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY ticket_id ORDER BY event_count, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        SIZE(COLLECT_SET(ticket_id) OVER (PARTITION BY event_count_2)) AS distinct_ticket_id_in_event_count_2
    FROM summarized_rows
    WINDOW w_ticket_id_by_event_count_tie AS (PARTITION BY ticket_id ORDER BY event_count)
)
SELECT *
FROM (
    SELECT event_at_quarter, event_type, distinct_ticket_id_in_event_count_2
    FROM sequenced
) src
PIVOT (
    SUM(distinct_ticket_id_in_event_count_2)
    FOR event_type IN ('escalated', 'replied', 'reopened', 'created')
)
