WITH base AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        DATE_FORMAT(release_date, 'yyyy-MM-dd') AS release_date_label,
        ADD_MONTHS(release_date, 3) AS release_date_plus_3m
    FROM tracks
    WHERE bpm >= 85
),
track_stats AS (
    -- Per-artist tracks metrics
    SELECT
        artist_id,
        COUNT(*) FILTER (WHERE is_explicit = FALSE) AS track_is_explicit_count
    FROM tracks
    GROUP BY artist_id
),
with_stats AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        COALESCE(t.track_is_explicit_count, 0) AS track_is_explicit_count
    FROM base s
    LEFT JOIN track_stats t ON t.artist_id = s.artist_id
),
detail_rows_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.moods,
        s.track_is_explicit_count,
        p.id AS play_id,
        p.played_at AS play_played_at,
        p.ms_played AS play_ms_played,
        p.source AS play_source
    FROM with_stats s
    LEFT JOIN plays p ON p.track_id = s.id
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(track_is_explicit_count, 2) OVER (PARTITION BY MONTH(release_date) ORDER BY play_played_at NULLS LAST, id, play_id) AS next2_track_is_explicit_count,
        ROUND(CUME_DIST() OVER w_all_by_duration_sec, 3) AS duration_sec_cume_dist
    FROM detail_rows_plays
    WINDOW w_all_by_duration_sec AS (ORDER BY duration_sec, id, play_id)
)
-- Subtotals with GROUPING SETS
SELECT
    next2_track_is_explicit_count,
    play_source,
    MAX(track_is_explicit_count) AS max_track_is_explicit_count,
    COUNT(*) AS row_count
FROM windowed
GROUP BY GROUPING SETS ((next2_track_is_explicit_count, play_source), (next2_track_is_explicit_count), (play_source), ())
