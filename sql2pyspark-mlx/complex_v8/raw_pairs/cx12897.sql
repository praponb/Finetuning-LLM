WITH filtered_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_type NOT IN ('reopened', 'assigned')
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        ticket_id,
        event_at,
        LAST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY event_type ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at,
        LAST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY event_type ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at_2,
        ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_at DESC, id) AS seq_no,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY ticket_id ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        ROW_NUMBER() OVER (PARTITION BY ticket_id ORDER BY event_at, id) AS event_at_seq,
        LAG(event_at) OVER (PARTITION BY ticket_id ORDER BY event_at, id) AS prev_event_at,
        LAG(event_at) OVER (PARTITION BY event_type ORDER BY event_at ASC, id) AS prev_event_at_2,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY event_type ORDER BY event_at DESC, id))) / 60, 1) AS minutes_since_prev_event_at,
        RANK() OVER (PARTITION BY ticket_id ORDER BY event_at, id) AS event_at_rank,
        LAG(event_at, 3) OVER (PARTITION BY ticket_id ORDER BY event_at, id) AS prev3_event_at,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY event_type ORDER BY event_at ASC, id))) / 60, 1) AS minutes_since_prev_event_at_2,
        LAST_VALUE(event_at) OVER (PARTITION BY event_type ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at
    FROM filtered_events
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        DATEDIFF(DATE '2025-06-30', prev3_event_at) AS days_since_prev3_event_at,
        FLOOR(seq_no / 2) * 2 AS seq_no_bucket
    FROM ranked
    WHERE seq_no BETWEEN 1 AND 10
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        event_at,
        last_known_event_at,
        last_known_event_at_2,
        seq_no,
        second_event_at,
        event_at_seq,
        prev_event_at,
        minutes_since_prev_event_at,
        minutes_since_prev_event_at_2,
        last_event_at,
        days_since_prev3_event_at,
        seq_no_bucket,
        SUM(days_since_prev3_event_at) OVER (PARTITION BY event_at_seq, DATE_TRUNC('MONTH', prev_event_at_2) ORDER BY prev_event_at_2 DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_days_since_prev3_event_at,
        LEAD(days_since_prev3_event_at, 2) OVER (PARTITION BY event_at_seq, DATE_TRUNC('MONTH', prev_event_at_2) ORDER BY prev_event_at_2 DESC, id) AS next2_days_since_prev3_event_at,
        ROW_NUMBER() OVER w_event_at_seq_prev_event_at_2_2 AS seq_no_2,
        LAST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY event_at_seq, DATE_TRUNC('MONTH', prev_event_at_2) ORDER BY prev_event_at_2 DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at_3,
        ROUND(seq_no - AVG(seq_no) OVER (PARTITION BY event_at_rank, seq_no), 2) AS seq_no_vs_event_at_rank_seq_no_avg,
        NTH_VALUE(prev3_event_at, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', last_known_event_at) ORDER BY event_at_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_prev3_event_at
    FROM scored
    WINDOW w_event_at_seq_prev_event_at_2_2 AS (PARTITION BY event_at_seq, DATE_TRUNC('MONTH', prev_event_at_2) ORDER BY prev_event_at_2 DESC, id)
)
SELECT
    id,
    last_known_event_at_2,
    seq_no,
    event_at_seq,
    prev_event_at,
    minutes_since_prev_event_at_2,
    last_event_at,
    days_since_prev3_event_at,
    running_days_since_prev3_event_at,
    next2_days_since_prev3_event_at,
    seq_no_2,
    last_known_event_at_3,
    seq_no_vs_event_at_rank_seq_no_avg,
    FLOOR(seq_no_bucket / 2) * 2 AS seq_no_bucket_bucket,
    ABS(event_at_seq - 6) AS event_at_seq_distance
FROM with_trends
ORDER BY days_since_prev3_event_at DESC NULLS FIRST
