WITH prepared AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        QUARTER(played_at) AS played_at_quarter,
        ABS(ms_played - 166517) AS ms_played_distance,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket
    FROM plays
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY track_id ORDER BY played_at_quarter DESC) AS played_at_quarter_dense_rank,
        LEAD(ms_played_distance) OVER (PARTITION BY track_id ORDER BY played_at_quarter DESC, id) AS next_ms_played_distance,
        FIRST_VALUE(ms_played_distance) IGNORE NULLS OVER (PARTITION BY track_id ORDER BY played_at_quarter DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_ms_played_distance,
        DENSE_RANK() OVER (PARTITION BY track_id ORDER BY played_at_quarter DESC, id) AS played_at_quarter_dense_rank_2,
        ROUND((UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(LAG(played_at) OVER (PARTITION BY track_id ORDER BY played_at_quarter DESC, id))) / 60, 1) AS minutes_since_prev_played_at
    FROM prepared
),
with_stats AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE NOT p.is_skipped) FROM plays p WHERE p.track_id = s.track_id) AS play_is_skipped_count
    FROM metrics s
),
filtered AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM with_stats s
    WHERE s.track_id IN (
            SELECT p.track_id
            FROM plays p
            WHERE p.device NOT IN ('speaker', 'desktop')
        )
)
SELECT
    id,
    listener_id,
    track_id,
    device,
    played_at_quarter,
    next_ms_played_distance,
    played_at_quarter_dense_rank_2,
    play_is_skipped_count,
    FLOOR(next_ms_played_distance / 50000) * 50000 AS next_ms_played_distance_bucket,
    IF(is_skipped, 'yes', 'no') AS is_skipped_label
FROM filtered
ORDER BY track_id, id
LIMIT 20
