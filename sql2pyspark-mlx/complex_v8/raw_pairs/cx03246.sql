WITH scoped_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_type IS NOT NULL
),
enriched AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        t.csat AS ticket_csat,
        t.opened_at AS ticket_opened_at,
        t.tags AS ticket_tags
    FROM scoped_events s
    JOIN tickets t ON t.id = s.ticket_id
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MIN(ticket_csat) OVER w_ticket_id AS ticket_id_min_ticket_csat,
        NTH_VALUE(ticket_opened_at, 2) OVER (PARTITION BY ticket_csat, ticket_id ORDER BY ticket_opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ticket_opened_at,
        ROW_NUMBER() OVER (PARTITION BY ticket_csat, ticket_id ORDER BY ticket_opened_at, id) AS rn,
        SIZE(COLLECT_SET(event_type) OVER w_ticket_id) AS distinct_event_type_in_ticket_id,
        RANK() OVER w_ticket_csat_ticket_id_by_tic_3 AS ticket_opened_at_rank,
        MAX(ticket_csat) OVER (PARTITION BY ticket_id) AS ticket_id_max_ticket_csat
    FROM enriched
    WINDOW w_ticket_id AS (PARTITION BY ticket_id), w_ticket_csat_ticket_id_by_tic_3 AS (PARTITION BY ticket_csat, ticket_id ORDER BY ticket_opened_at)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(ticket_csat - AVG(ticket_csat) OVER w_ticket_opened_at_rank_event, 2) AS ticket_csat_vs_ticket_opened_at_rank_event_type_,
        LAST_VALUE(second_ticket_opened_at) IGNORE NULLS OVER (ORDER BY ticket_opened_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_second_ticket_opened_at,
        ticket_id_max_ticket_csat - LAG(ticket_id_max_ticket_csat) OVER w_distinct_event_type_in_ticke AS ticket_id_max_ticket_csat_change,
        SUM(ticket_id_min_ticket_csat) OVER w_ticket_opened_at_rank_event AS ticket_opened_at_rank_event_type_sum_ticket_id_m
    FROM with_trends
    WINDOW w_ticket_opened_at_rank_event AS (PARTITION BY ticket_opened_at_rank, event_type), w_distinct_event_type_in_ticke AS (PARTITION BY distinct_event_type_in_ticket_id ORDER BY rn DESC NULLS FIRST, id)
),
detail_rows_events AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.ticket_csat,
        s.ticket_opened_at,
        s.rn,
        s.distinct_event_type_in_ticket_id,
        s.ticket_opened_at_rank,
        s.ticket_id_max_ticket_csat,
        s.ticket_csat_vs_ticket_opened_at_rank_event_type_,
        s.last_known_second_ticket_opened_at,
        s.ticket_id_max_ticket_csat_change,
        t.id AS event_id,
        t.event_at,
        t.event_type AS event_event_type
    FROM metrics s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id AND t.event_at >= TIMESTAMP '2025-01-13 12:00:00'
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        distinct_event_type_in_ticket_id,
        DATE_TRUNC('MONTH', ticket_opened_at) AS ticket_opened_at_month,
        ARRAY_SORT(COLLECT_SET(event_type)) AS event_type_list,
        ROUND(SUM(ticket_csat) / NULLIF(SUM(ticket_id_max_ticket_csat_change), 0), 3) AS ticket_csat_to_ticket_id_max_ticket_csat_change_,
        SUM(distinct_event_type_in_ticket_id) AS total_distinct_event_type_in_ticket_id,
        SUM(CASE WHEN event_type IS NOT NULL THEN 1 ELSE 0 END) AS n_event_type_is_not_null,
        COUNT(*) AS n_rows
    FROM detail_rows_events
    GROUP BY distinct_event_type_in_ticket_id, DATE_TRUNC('MONTH', ticket_opened_at)
)
SELECT
    distinct_event_type_in_ticket_id,
    ticket_opened_at_month,
    event_type_list,
    ticket_csat_to_ticket_id_max_ticket_csat_change_,
    total_distinct_event_type_in_ticket_id,
    n_event_type_is_not_null,
    DATEDIFF(DATE '2026-01-01', ticket_opened_at_month) AS days_since_ticket_opened_at_month,
    FLOOR(n_event_type_is_not_null / 2) * 2 AS n_event_type_is_not_null_bucket
FROM totals
ORDER BY n_event_type_is_not_null_bucket, ticket_opened_at_month NULLS LAST
