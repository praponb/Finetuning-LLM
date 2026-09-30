WITH cleaned AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped
    FROM plays
    WHERE (NOT is_skipped OR ms_played < 239955)
),
play_stats AS (
    -- Per-listener plays metrics
    SELECT listener_id, MAX(ms_played) AS max_play_ms_played
    FROM plays
    GROUP BY listener_id
),
with_counts AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.max_play_ms_played
    FROM cleaned s
    LEFT JOIN play_stats p ON p.listener_id = s.listener_id
),
play_stats_2 AS (
    -- Per-track plays metrics
    SELECT track_id, COUNT(*) AS play_count, SUM(ms_played) AS total_play_ms_played
    FROM plays
    GROUP BY track_id
),
with_stats AS (
    -- Attach plays metrics
    SELECT
        s.*,
        COALESCE(p.play_count, 0) AS play_count,
        p.total_play_ms_played
    FROM with_counts s
    LEFT JOIN play_stats_2 p ON p.track_id = s.track_id
),
with_details AS (
    -- Join tracks
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.is_skipped,
        s.max_play_ms_played,
        s.play_count,
        s.total_play_ms_played,
        t.is_explicit AS track_is_explicit,
        t.moods AS track_moods
    FROM with_stats s
    JOIN tracks t ON t.id = s.track_id AND NOT t.is_explicit
    WHERE ARRAY_CONTAINS(t.moods, 'chill')
)
SELECT
    id,
    listener_id,
    track_id,
    max_play_ms_played,
    play_count,
    total_play_ms_played,
    track_moods
FROM with_details
ORDER BY max_play_ms_played NULLS LAST
