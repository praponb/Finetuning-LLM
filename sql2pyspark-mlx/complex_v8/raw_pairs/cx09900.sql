WITH valid_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source
    FROM plays
),
play_stats AS (
    -- Per-track plays metrics
    SELECT
        track_id,
        SUM(ms_played) AS total_play_ms_played,
        MIN(ms_played) AS min_play_ms_played,
        COUNT(CASE WHEN device <> 'speaker' THEN 1 END) AS play_device_count
    FROM plays
    GROUP BY track_id
),
with_counts AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.total_play_ms_played,
        p.min_play_ms_played,
        COALESCE(p.play_device_count, 0) AS play_device_count
    FROM valid_plays s
    LEFT JOIN play_stats p ON p.track_id = s.track_id
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        play_device_count,
        DATE_TRUNC('MONTH', played_at) AS played_at_month,
        listener_id,
        SUM(play_device_count) AS total_play_device_count,
        COUNT(*) AS cnt,
        ROUND(STDDEV(min_play_ms_played), 2) AS stddev_min_play_ms_played
    FROM with_counts
    GROUP BY play_device_count, DATE_TRUNC('MONTH', played_at), listener_id
),
profiled AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT MAX(p.ms_played) FROM plays p WHERE p.listener_id = s.listener_id) AS max_play_ms_played,
        (SELECT MIN(p.played_at) FROM plays p WHERE p.listener_id = s.listener_id) AS first_played_at
    FROM rollup_base s
)
SELECT
    play_device_count,
    played_at_month,
    listener_id,
    total_play_device_count,
    cnt,
    stddev_min_play_ms_played,
    max_play_ms_played,
    first_played_at,
    ABS(max_play_ms_played - 261750) AS max_play_ms_played_distance
FROM profiled
WHERE total_play_device_count <= 34
ORDER BY stddev_min_play_ms_played DESC NULLS FIRST, max_play_ms_played
