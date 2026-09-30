WITH recent AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source,
        DATE_FORMAT(played_at, 'MMM yyyy') AS played_at_label
    FROM plays
    WHERE track_id IS NOT NULL
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY device, track_id ORDER BY played_at DESC, id) AS played_at_dense_rank,
        RANK() OVER (PARTITION BY device, track_id ORDER BY played_at DESC, id) AS played_at_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY track_id ORDER BY played_at, id), 3) AS played_at_percent_rank,
        NTILE(10) OVER w_track_id_by_played_at AS played_at_decile,
        LAG(played_at, 2) OVER w_track_id_by_played_at AS prev2_played_at,
        FIRST_VALUE(played_at) OVER (ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_played_at,
        LEAD(played_at) OVER (PARTITION BY track_id ORDER BY played_at, id) AS next_played_at,
        ROUND((UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(LAG(played_at) OVER (PARTITION BY track_id ORDER BY played_at, id))) / 60, 1) AS minutes_since_prev_played_at,
        LAG(played_at) OVER (PARTITION BY device ORDER BY played_at DESC, id) AS prev_played_at,
        LAST_VALUE(played_at) OVER (ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_played_at,
        NTILE(5) OVER (PARTITION BY device, track_id ORDER BY played_at DESC, id) AS played_at_quintile
    FROM recent
    WINDOW w_track_id_by_played_at AS (PARTITION BY track_id ORDER BY played_at, id)
)
-- Final projection
SELECT
    id,
    listener_id,
    is_skipped,
    source,
    played_at_dense_rank,
    played_at_rank,
    played_at_percent_rank,
    played_at_decile,
    prev2_played_at,
    first_played_at,
    next_played_at,
    minutes_since_prev_played_at,
    prev_played_at,
    CONCAT(source, '#', CAST(played_at_dense_rank AS STRING)) AS source_ref,
    DATEDIFF(DATE '2026-01-01', next_played_at) AS days_since_next_played_at
FROM sequenced
ORDER BY first_played_at DESC
