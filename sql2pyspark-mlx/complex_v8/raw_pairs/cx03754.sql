WITH base_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        bpm,
        GREATEST(duration_sec, bpm) AS greatest_duration_sec_bpm
    FROM tracks
),
filtered AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM base_tracks s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.id
              AND p.device IN ('car', 'phone')
        )
      AND s.duration_sec >= 184
),
with_counts AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM tracks t WHERE t.artist_id = s.artist_id) AS track_count,
        (SELECT COUNT(*) FROM tracks t WHERE t.artist_id = s.artist_id) AS track_count_2
    FROM filtered s
),
with_activity AS (
    -- Rows with no matching tracks
    SELECT s.*
    FROM with_counts s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
              AND SIZE(t.moods) >= 2
        )
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        bpm,
        greatest_duration_sec_bpm,
        track_count,
        track_count_2,
        MAX(track_count_2) OVER w_track_count_2 AS track_count_2_max_track_count_2,
        ROUND(track_count_2 - AVG(track_count_2) OVER w_track_count_2, 2) AS track_count_2_vs_track_count_2_avg
    FROM with_activity
    WINDOW w_track_count_2 AS (PARTITION BY track_count_2)
),
combined_details_artist AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.is_explicit,
        s.bpm,
        s.greatest_duration_sec_bpm,
        s.track_count_2,
        s.track_count_2_max_track_count_2,
        s.track_count_2_vs_track_count_2_avg,
        a.genre AS artist_genre,
        a.artist_name
    FROM with_windows s
    JOIN artists a ON a.id = s.artist_id
    WHERE UPPER(TRIM(a.artist_name)) LIKE 'K%'
),
filtered_2 AS (
    -- Compare against the overall avg
    SELECT *
    FROM combined_details_artist
    WHERE track_count_2_max_track_count_2 > (SELECT AVG(track_count_2_max_track_count_2) FROM combined_details_artist)
)
-- Final projection
SELECT
    id,
    artist_id,
    duration_sec,
    is_explicit,
    bpm,
    greatest_duration_sec_bpm,
    track_count_2,
    track_count_2_max_track_count_2,
    track_count_2_vs_track_count_2_avg,
    artist_genre,
    LOWER(REGEXP_REPLACE(TRIM(artist_genre), ' ', '_')) AS artist_genre_slug
FROM filtered_2
ORDER BY artist_genre DESC NULLS FIRST
