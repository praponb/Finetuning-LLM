WITH base_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_at < TIMESTAMP '2025-02-24 13:00:00'
      AND actor IN ('system', 'customer')
),
tagged AS (
    -- Derive helper columns
    SELECT *, event_at + INTERVAL 2 HOURS AS event_at_plus_2h
    FROM base_events
    WHERE actor IN ('system', 'agent')
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        COUNT(CASE WHEN event_type = 'replied' THEN 1 END) AS event_event_type_count,
        COUNT(*) AS event_count
    FROM ticket_events
    GROUP BY ticket_id
),
with_counts AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        COALESCE(t.event_event_type_count, 0) AS event_event_type_count,
        COALESCE(t.event_count, 0) AS event_count
    FROM tagged s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_at_plus_2h,
        event_event_type_count,
        event_count,
        NTH_VALUE(event_count, 2) OVER (PARTITION BY event_event_type_count, event_count ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_count,
        ROUND(PERCENT_RANK() OVER (PARTITION BY event_count, ticket_id ORDER BY event_at_plus_2h ASC), 3) AS event_at_plus_2h_percent_rank,
        ROUND(event_count - AVG(event_count) OVER w_event_at_plus_2h_month_ticke, 2) AS event_count_vs_event_at_plus_2h_month_ticket_id_,
        DENSE_RANK() OVER (PARTITION BY ticket_id, event_count ORDER BY event_at, id) AS event_at_dense_rank,
        ROW_NUMBER() OVER (PARTITION BY event_event_type_count, event_count ORDER BY event_at, id) AS seq_no,
        LEAD(event_event_type_count) OVER (PARTITION BY event_event_type_count, event_count ORDER BY event_at, id) AS next_event_event_type_count
    FROM with_counts
    WINDOW w_event_at_plus_2h_month_ticke AS (PARTITION BY MONTH(event_at_plus_2h), ticket_id)
)
SELECT
    second_event_count,
    next_event_event_type_count,
    actor,
    GROUPING(second_event_count) AS is_total_second_event_count,
    SUM(CASE WHEN actor IS NOT NULL THEN event_at_plus_2h_percent_rank ELSE 0 END) AS event_at_plus_2h_percent_rank_actor_is_not_null,
    COUNT(*) AS n_rows
FROM ranked
GROUP BY CUBE(second_event_count, next_event_event_type_count, actor)
ORDER BY second_event_count NULLS LAST, next_event_event_type_count NULLS LAST, actor NULLS LAST
