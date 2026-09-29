WITH cleaned_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        bpm,
        moods
    FROM tracks
    WHERE release_date BETWEEN DATE '2024-04-21' AND DATE '2025-04-02'
      AND duration_sec > 142
),
track_stats AS (
    -- Per-artist tracks metrics
    SELECT artist_id, MIN(duration_sec) AS min_track_duration_sec
    FROM tracks
    GROUP BY artist_id
),
summarized_rows AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        t.min_track_duration_sec
    FROM cleaned_tracks s
    LEFT JOIN track_stats t ON t.artist_id = s.artist_id
),
play_stats AS (
    -- Per-track plays metrics
    SELECT
        track_id,
        MAX(played_at) AS last_played_at,
        COUNT(*) AS play_count,
        MIN(ms_played) AS min_play_ms_played
    FROM plays
    GROUP BY track_id
),
summarized_rows_2 AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.last_played_at,
        COALESCE(p.play_count, 0) AS play_count,
        p.min_play_ms_played
    FROM summarized_rows s
    LEFT JOIN play_stats p ON p.track_id = s.id
),
windowed AS (
    -- Running and moving aggregates
    SELECT *, COUNT(*) OVER (PARTITION BY is_explicit) AS rows_in_is_explicit
    FROM summarized_rows_2
),
artist_id_stats AS (
    -- Max min_play_ms_played per artist_id
    SELECT artist_id, MAX(min_play_ms_played) AS max_min_play_ms_played
    FROM windowed
    GROUP BY artist_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_min_play_ms_played,
        ROUND(s.min_play_ms_played - g.max_min_play_ms_played, 2) AS min_play_ms_played_minus_max
    FROM windowed s
    JOIN artist_id_stats g ON g.artist_id = s.artist_id
    WHERE s.min_play_ms_played = g.max_min_play_ms_played
),
screened AS (
    -- Rows with no matching tracks
    SELECT s.*
    FROM above_average s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
              AND t.bpm >= 82
              AND SIZE(t.moods) >= 2
        )
      AND TO_DATE(s.last_played_at) < DATE '2025-02-13'
),
track_stats_2 AS (
    -- Per-artist tracks metrics
    SELECT
        artist_id,
        MIN(release_date) AS first_track_release_date,
        MIN(bpm) AS min_track_bpm
    FROM tracks
    GROUP BY artist_id
),
profiled AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        t.first_track_release_date,
        t.min_track_bpm
    FROM screened s
    LEFT JOIN track_stats_2 t ON t.artist_id = s.artist_id
)
-- Subtotals with ROLLUP
SELECT
    artist_id,
    is_explicit,
    GROUPING_ID() AS gid,
    ROUND(SUM(max_min_play_ms_played) / NULLIF(SUM(min_play_ms_played), 0), 3) AS max_min_play_ms_played_to_min_play_ms_played_rat
FROM profiled
GROUP BY ROLLUP(artist_id, is_explicit)
ORDER BY artist_id NULLS LAST, is_explicit NULLS LAST
