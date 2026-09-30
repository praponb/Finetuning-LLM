WITH prepared_plays AS (
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
    WHERE ms_played <= 290754
      AND played_at >= TIMESTAMP '2025-02-06 11:00:00'
),
play_stats AS (
    -- Per-track plays metrics
    SELECT
        track_id,
        MAX(played_at) AS last_played_at,
        COUNT(*) AS play_count,
        MIN(played_at) AS first_played_at
    FROM plays
    GROUP BY track_id
),
profiled AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.last_played_at,
        COALESCE(p.play_count, 0) AS play_count,
        p.first_played_at
    FROM prepared_plays s
    LEFT JOIN play_stats p ON p.track_id = s.track_id
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.is_skipped,
        s.source,
        s.last_played_at,
        s.play_count,
        s.first_played_at,
        t.duration_sec AS track_duration_sec
    FROM profiled s
    LEFT JOIN tracks t ON t.id = s.track_id AND t.release_date >= DATE '2024-11-14'
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM with_parent
    WHERE track_duration_sec > (SELECT AVG(track_duration_sec) FROM with_parent)
)
SELECT id, metric, metric_value
FROM (
    SELECT id, track_duration_sec, play_count
    FROM qualified
) src
UNPIVOT (
    metric_value FOR metric IN (track_duration_sec, play_count)
) unp
