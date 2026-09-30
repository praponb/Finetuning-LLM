WITH prepared AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE actor = 'agent'
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        event_at,
        event_type,
        LAG(event_at, 2) OVER (ORDER BY event_at DESC, id) AS prev2_event_at,
        DENSE_RANK() OVER (PARTITION BY event_type ORDER BY event_at NULLS LAST) AS event_at_dense_rank,
        LAST_VALUE(event_at) IGNORE NULLS OVER (ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at,
        NTH_VALUE(event_at, 2) OVER (ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at
    FROM prepared
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(event_at_dense_rank - AVG(event_at_dense_rank) OVER (PARTITION BY TO_DATE(event_at)), 2) AS event_at_dense_rank_vs_event_at_to_date_avg,
        ROUND((UNIX_TIMESTAMP(prev2_event_at) - UNIX_TIMESTAMP(LAG(prev2_event_at) OVER w_event_at_dense_rank_event_ty)) / 60, 1) AS minutes_since_prev_prev2_event_at,
        LAG(event_at) OVER (PARTITION BY event_at_dense_rank, event_type ORDER BY prev2_event_at DESC NULLS FIRST, id) AS prev_event_at,
        SIZE(COLLECT_SET(event_type) OVER (PARTITION BY TO_DATE(event_at))) AS distinct_event_type_in_event_at_to_date
    FROM windowed
    WINDOW w_event_at_dense_rank_event_ty AS (PARTITION BY event_at_dense_rank, event_type ORDER BY prev2_event_at DESC NULLS FIRST, id)
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        DATE_FORMAT(event_at, 'yyyy-MM-dd HH:00') AS event_at_label,
        FLOOR(event_at_dense_rank / 2) * 2 AS event_at_dense_rank_bucket
    FROM sequenced
)
-- Final projection
SELECT
    id,
    event_at,
    event_type,
    prev2_event_at,
    event_at_dense_rank,
    last_known_event_at,
    event_at_dense_rank_vs_event_at_to_date_avg,
    minutes_since_prev_prev2_event_at,
    prev_event_at,
    distinct_event_type_in_event_at_to_date,
    event_at_label,
    event_at_dense_rank_bucket,
    CONCAT(event_type, '#', CAST(event_at_dense_rank AS STRING)) AS event_type_ref
FROM annotated
WHERE minutes_since_prev_prev2_event_at <= 3000.0
ORDER BY minutes_since_prev_prev2_event_at DESC
