WITH cleaned_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATEDIFF(DATE '2026-01-01', event_at) AS days_since_event_at,
        event_at + INTERVAL 15 MINUTES AS event_at_plus_15m
    FROM ticket_events
    WHERE HOUR(event_at) BETWEEN 10 AND 13
      AND actor IN ('agent', 'customer')
),
combined_details_ticket AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.days_since_event_at,
        t.csat AS ticket_csat,
        t.priority AS ticket_priority
    FROM cleaned_events s
    JOIN tickets t ON t.id = s.ticket_id
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        ticket_id,
        event_at,
        days_since_event_at,
        ticket_csat,
        ticket_priority,
        LAG(event_at, 3) OVER (ORDER BY days_since_event_at, id) AS prev3_event_at,
        SIZE(COLLECT_SET(event_type) OVER w_ticket_id_ticket_priority) AS distinct_event_type_in_ticket_id_ticket_priority,
        RANK() OVER (PARTITION BY ticket_id, event_type ORDER BY ticket_csat DESC NULLS FIRST) AS ticket_csat_rank,
        ROUND(PERCENT_RANK() OVER w_event_type_ticket_csat_by_da, 3) AS days_since_event_at_percent_rank,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY event_type, ticket_csat ORDER BY days_since_event_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        DENSE_RANK() OVER (PARTITION BY ticket_id, event_type ORDER BY ticket_csat DESC NULLS FIRST, id) AS ticket_csat_dense_rank,
        ROUND(CUME_DIST() OVER (ORDER BY days_since_event_at), 3) AS days_since_event_at_cume_dist,
        LAST_VALUE(event_at) OVER (PARTITION BY ticket_id, event_type ORDER BY ticket_csat DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at,
        LEAD(days_since_event_at, 1, 0) OVER (ORDER BY days_since_event_at, id) AS next_days_since_event_at,
        COUNT(*) OVER w_ticket_id_ticket_priority AS rows_in_ticket_id_ticket_priority,
        ROUND(days_since_event_at - AVG(days_since_event_at) OVER (PARTITION BY ticket_id, ticket_priority), 2) AS days_since_event_at_vs_ticket_id_ticket_priority,
        MAX(ticket_csat) OVER (ORDER BY days_since_event_at, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS ticket_csat_max_2,
        COUNT(*) OVER (ORDER BY days_since_event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count
    FROM combined_details_ticket
    WINDOW w_ticket_id_ticket_priority AS (PARTITION BY ticket_id, ticket_priority), w_event_type_ticket_csat_by_da AS (PARTITION BY event_type, ticket_csat ORDER BY days_since_event_at DESC NULLS FIRST, id)
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        ticket_csat,
        ticket_priority,
        prev3_event_at,
        distinct_event_type_in_ticket_id_ticket_priority,
        second_event_at,
        ticket_csat_dense_rank,
        days_since_event_at_cume_dist,
        next_days_since_event_at,
        rows_in_ticket_id_ticket_priority,
        running_count,
        ROUND((UNIX_TIMESTAMP(second_event_at) - UNIX_TIMESTAMP(prev3_event_at)) / 3600, 1) AS hours_prev3_event_at_to_second_event_at
    FROM windowed
    WHERE days_since_event_at < 455
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_all_by_prev3_event_at, 3) AS prev3_event_at_percent_rank,
        ROUND(AVG(next_days_since_event_at) OVER (PARTITION BY distinct_event_type_in_ticket_id_ticket_priority ORDER BY ticket_csat_dense_rank DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS next_days_since_event_at_avg_4,
        ROUND(PERCENT_RANK() OVER (PARTITION BY ticket_id ORDER BY event_at NULLS LAST), 3) AS event_at_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY distinct_event_type_in_ticket_id_ticket_priority ORDER BY ticket_csat_dense_rank DESC, id) AS ticket_csat_dense_rank_seq,
        ROUND(CUME_DIST() OVER w_distinct_event_type_in_ticke_3, 3) AS ticket_csat_dense_rank_cume_dist,
        MAX(next_days_since_event_at) OVER (PARTITION BY ticket_csat_dense_rank) AS ticket_csat_dense_rank_max_next_days_since_event,
        ROUND((UNIX_TIMESTAMP(second_event_at) - UNIX_TIMESTAMP(LAG(second_event_at) OVER w_distinct_event_type_in_ticke_2)) / 60, 1) AS minutes_since_prev_second_event_at
    FROM enriched
    WINDOW w_all_by_prev3_event_at AS (ORDER BY prev3_event_at DESC, id), w_distinct_event_type_in_ticke_3 AS (PARTITION BY distinct_event_type_in_ticket_id_ticket_priority ORDER BY ticket_csat_dense_rank DESC), w_distinct_event_type_in_ticke_2 AS (PARTITION BY distinct_event_type_in_ticket_id_ticket_priority ORDER BY ticket_csat_dense_rank DESC, id)
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    ticket_csat,
    ticket_priority,
    second_event_at,
    days_since_event_at_cume_dist,
    next_days_since_event_at,
    rows_in_ticket_id_ticket_priority,
    running_count,
    prev3_event_at_percent_rank,
    next_days_since_event_at_avg_4,
    event_at_percent_rank,
    ticket_csat_dense_rank_cume_dist,
    minutes_since_prev_second_event_at,
    CASE WHEN ticket_priority IN ('P2', 'P4') THEN 'group_a' WHEN ticket_priority = 'P1' THEN 'group_b' ELSE 'other' END AS ticket_priority_group,
    LOWER(TRIM(ticket_priority)) AS ticket_priority_clean
FROM metrics
WHERE distinct_event_type_in_ticket_id_ticket_priority >= 3
ORDER BY id DESC, next_days_since_event_at_avg_4 DESC
LIMIT 10
