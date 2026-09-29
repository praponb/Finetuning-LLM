WITH base_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE HOUR(event_at) BETWEEN 10 AND 12
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        DENSE_RANK() OVER (ORDER BY event_at DESC) AS event_at_dense_rank,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY event_type ORDER BY event_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at_2,
        ROUND(CUME_DIST() OVER w_event_type_by_event_at, 3) AS event_at_cume_dist,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (ORDER BY event_at, id))) / 60, 1) AS minutes_since_prev_event_at,
        DENSE_RANK() OVER w_event_type_by_event_at_ties AS event_at_dense_rank_2,
        FIRST_VALUE(event_at) OVER (ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        ROW_NUMBER() OVER w_all_by_event_at AS seq_no,
        LAST_VALUE(event_at) IGNORE NULLS OVER (ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at
    FROM base_events
    WINDOW w_event_type_by_event_at AS (PARTITION BY event_type ORDER BY event_at NULLS LAST, id), w_event_type_by_event_at_ties AS (PARTITION BY event_type ORDER BY event_at NULLS LAST), w_all_by_event_at AS (ORDER BY event_at ASC, id)
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY event_type, actor ORDER BY event_at_dense_rank_2, id))) / 60, 1) AS minutes_since_prev_event_at_2,
        RANK() OVER w_event_at_dense_rank_by_seq_n AS seq_no_rank,
        NTILE(4) OVER (PARTITION BY seq_no ORDER BY event_at ASC, id) AS event_at_quartile,
        MAX(seq_no) OVER (PARTITION BY ticket_id, TO_DATE(first_event_at)) AS ticket_id_first_event_at_to_date_max_seq_no,
        ROUND(event_at_cume_dist - AVG(event_at_cume_dist) OVER (PARTITION BY ticket_id, TO_DATE(first_event_at)), 2) AS event_at_cume_dist_vs_ticket_id_first_event_at_t,
        SUM(event_at_dense_rank_2) OVER (PARTITION BY event_type, actor ORDER BY event_at_dense_rank_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_event_at_dense_rank_2,
        ROUND(PERCENT_RANK() OVER (PARTITION BY event_type, actor ORDER BY event_at_dense_rank_2), 3) AS event_at_dense_rank_2_percent_rank,
        ROUND(event_at_dense_rank - AVG(event_at_dense_rank) OVER (PARTITION BY ticket_id, TO_DATE(first_event_at)), 2) AS event_at_dense_rank_vs_ticket_id_first_event_at_
    FROM metrics
    WINDOW w_event_at_dense_rank_by_seq_n AS (PARTITION BY event_at_dense_rank ORDER BY seq_no DESC)
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        event_at_dense_rank,
        event_at_dense_rank_2,
        ROUND(AVG(minutes_since_prev_event_at_2), 2) AS avg_minutes_since_prev_event_at_2,
        ROUND(AVG(seq_no_rank), 2) AS avg_seq_no_rank,
        SUM(CASE WHEN event_type IN ('escalated', 'replied') THEN minutes_since_prev_event_at ELSE 0 END) AS minutes_since_prev_event_at_event_type_in_escala,
        COUNT(*) AS n_rows,
        MAX(event_at_cume_dist_vs_ticket_id_first_event_at_t) AS max_event_at_cume_dist_vs_ticket_id_first_event_,
        ROUND(AVG(event_at_dense_rank_2), 2) AS avg_event_at_dense_rank_2
    FROM ranked
    GROUP BY event_at_dense_rank, event_at_dense_rank_2
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(event_at_dense_rank) OVER (PARTITION BY event_at_dense_rank_2, n_rows ORDER BY avg_event_at_dense_rank_2 DESC NULLS FIRST, event_at_dense_rank, event_at_dense_rank_2 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS event_at_dense_rank_avg_3,
        LAG(event_at_dense_rank_2, 1, 0) OVER (PARTITION BY n_rows, event_at_dense_rank_2 ORDER BY avg_minutes_since_prev_event_at_2, event_at_dense_rank, event_at_dense_rank_2) AS prev_event_at_dense_rank_2
    FROM totals
)
SELECT
    event_at_dense_rank,
    avg_minutes_since_prev_event_at_2,
    avg_seq_no_rank,
    minutes_since_prev_event_at_event_type_in_escala,
    n_rows,
    avg_event_at_dense_rank_2,
    event_at_dense_rank_avg_3,
    prev_event_at_dense_rank_2,
    COALESCE(event_at_dense_rank_2, 0) AS event_at_dense_rank_2_filled,
    CASE WHEN n_rows >= 8 THEN 'HIGH' WHEN n_rows >= 4 THEN 'MEDIUM' ELSE 'LOW' END AS n_rows_tier
FROM with_windows
ORDER BY event_at_dense_rank_2_filled NULLS LAST
