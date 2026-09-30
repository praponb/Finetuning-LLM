WITH base AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT(event_type, '#', CAST(ticket_id AS STRING)) AS event_type_ref
    FROM ticket_events
    WHERE TO_DATE(event_at) < DATE '2025-03-08'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY event_type ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at,
        ROW_NUMBER() OVER w_event_at_month_by_event_at AS rn,
        LAG(event_at, 2) OVER (PARTITION BY TO_DATE(event_at), event_type ORDER BY event_at, id) AS prev2_event_at,
        LAST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('MONTH', event_at) ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at_2,
        DENSE_RANK() OVER (PARTITION BY TO_DATE(event_at), event_type ORDER BY event_at, id) AS event_at_dense_rank,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY ticket_id, actor ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY ticket_id, actor ORDER BY event_at, id), 3) AS event_at_percent_rank,
        LEAD(event_at, 3) OVER w_event_at_to_date_event_type AS next3_event_at,
        NTILE(3) OVER w_event_at_to_date_by_event_at AS event_at_tercile,
        ROUND(CUME_DIST() OVER (PARTITION BY event_type ORDER BY event_at DESC, id), 3) AS event_at_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY ticket_id, actor ORDER BY event_at, id) AS rn_2,
        ROUND(CUME_DIST() OVER (PARTITION BY ticket_id, actor ORDER BY event_at), 3) AS event_at_cume_dist_2,
        ROUND(PERCENT_RANK() OVER w_event_at_month_by_event_at_t, 3) AS event_at_percent_rank_2,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER w_event_type_by_event_at)) / 60, 1) AS minutes_since_prev_event_at
    FROM base
    WINDOW w_event_at_month_by_event_at AS (PARTITION BY DATE_TRUNC('MONTH', event_at) ORDER BY event_at, id), w_event_at_to_date_event_type AS (PARTITION BY TO_DATE(event_at), event_type ORDER BY event_at, id), w_event_at_to_date_by_event_at AS (PARTITION BY TO_DATE(event_at) ORDER BY event_at DESC, id), w_event_at_month_by_event_at_t AS (PARTITION BY DATE_TRUNC('MONTH', event_at) ORDER BY event_at), w_event_type_by_event_at AS (PARTITION BY event_type ORDER BY event_at DESC, id)
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        event_at,
        event_type,
        actor,
        event_type_ref,
        last_known_event_at,
        rn,
        last_known_event_at_2,
        event_at_dense_rank,
        first_known_event_at,
        event_at_percent_rank,
        next3_event_at,
        rn_2,
        event_at_percent_rank_2,
        minutes_since_prev_event_at,
        LAST_VALUE(event_at_tercile) IGNORE NULLS OVER (PARTITION BY rn_2 ORDER BY event_at_tercile DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at_tercile,
        SUM(event_at_percent_rank) OVER (PARTITION BY event_type) AS event_type_sum_event_at_percent_rank,
        LEAD(rn_2, 2) OVER (ORDER BY first_known_event_at ASC, id) AS next2_rn_2,
        SIZE(COLLECT_SET(ticket_id) OVER w_event_type) AS distinct_ticket_id_in_event_type,
        FIRST_VALUE(rn_2) IGNORE NULLS OVER (ORDER BY first_known_event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_rn_2,
        ROUND(CUME_DIST() OVER (ORDER BY first_known_event_at ASC), 3) AS first_known_event_at_cume_dist,
        SUM(event_at_dense_rank) OVER (PARTITION BY event_type) AS event_type_sum_event_at_dense_rank
    FROM metrics
    WINDOW w_event_type AS (PARTITION BY event_type)
)
-- Final projection
SELECT
    id,
    event_at,
    actor,
    last_known_event_at,
    rn,
    last_known_event_at_2,
    first_known_event_at,
    event_at_percent_rank,
    next3_event_at,
    rn_2,
    event_at_percent_rank_2,
    minutes_since_prev_event_at,
    last_known_event_at_tercile,
    next2_rn_2,
    distinct_ticket_id_in_event_type,
    first_known_rn_2,
    first_known_event_at_cume_dist,
    event_type_sum_event_at_dense_rank,
    CONCAT_WS('-', event_type_ref, event_type) AS event_type_ref_event_type_label
FROM windowed
