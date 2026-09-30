WITH valid AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        bpm,
        moods,
        CASE WHEN duration_sec >= 365 THEN 'A' WHEN duration_sec >= 183 THEN 'B' ELSE 'C' END AS duration_sec_tier,
        CONCAT(title, '#', CAST(duration_sec AS STRING)) AS title_ref,
        CONCAT(title, '#', CAST(duration_sec AS STRING)) AS title_ref_2
    FROM tracks
    WHERE ARRAY_CONTAINS(moods, 'workout')
      AND artist_id IS NOT NULL
),
artist_id_stats AS (
    -- Median bpm per artist_id
    SELECT artist_id, PERCENTILE_APPROX(bpm, 0.5) AS median_bpm
    FROM valid
    GROUP BY artist_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.median_bpm, ROUND(s.bpm - g.median_bpm, 2) AS bpm_minus_median
    FROM valid s
    JOIN artist_id_stats g ON g.artist_id = s.artist_id
    WHERE s.bpm >= g.median_bpm
),
play_stats AS (
    -- Per-track plays metrics
    SELECT
        track_id,
        COUNT(*) FILTER (WHERE NOT is_skipped) AS play_is_skipped_count,
        COUNT(*) AS play_count
    FROM plays
    GROUP BY track_id
),
profiled AS (
    -- Attach plays metrics
    SELECT
        s.*,
        COALESCE(p.play_is_skipped_count, 0) AS play_is_skipped_count,
        COALESCE(p.play_count, 0) AS play_count
    FROM outliers s
    LEFT JOIN play_stats p ON p.track_id = s.id
),
summarized_rows AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        (SELECT MIN(t.release_date) FROM tracks t WHERE t.artist_id = s.artist_id) AS first_track_release_date,
        (SELECT MIN(t.bpm) FROM tracks t WHERE t.artist_id = s.artist_id) AS min_track_bpm,
        (SELECT COUNT(*) FILTER (WHERE t.is_explicit = FALSE) FROM tracks t WHERE t.artist_id = s.artist_id) AS track_is_explicit_count
    FROM profiled s
),
combined_details_artist AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.is_explicit,
        s.bpm,
        s.moods,
        s.duration_sec_tier,
        s.title_ref_2,
        s.median_bpm,
        s.play_count,
        s.min_track_bpm,
        a.genre AS artist_genre,
        a.debut_year AS artist_debut_year
    FROM summarized_rows s
    JOIN artists a ON a.id = s.artist_id
),
track_stats_2 AS (
    -- Per-artist tracks metrics
    SELECT artist_id, COUNT(*) AS track_count
    FROM tracks
    GROUP BY artist_id
),
summarized_rows_2 AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        COALESCE(t.track_count, 0) AS track_count
    FROM combined_details_artist s
    LEFT JOIN track_stats_2 t ON t.artist_id = s.artist_id
),
track_stats_3 AS (
    -- Per-artist tracks metrics
    SELECT
        artist_id,
        COUNT(*) FILTER (WHERE is_explicit = TRUE) AS track_is_explicit_count,
        COUNT(*) AS track_count_2,
        ROUND(AVG(duration_sec), 2) AS avg_track_duration_sec
    FROM tracks
    GROUP BY artist_id
),
profiled_2 AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        COALESCE(t.track_is_explicit_count, 0) AS track_is_explicit_count,
        COALESCE(t.track_count_2, 0) AS track_count_2,
        t.avg_track_duration_sec
    FROM summarized_rows_2 s
    LEFT JOIN track_stats_3 t ON t.artist_id = s.artist_id
)
-- Final projection
SELECT
    id,
    artist_id,
    title,
    is_explicit,
    bpm,
    moods,
    duration_sec_tier,
    title_ref_2,
    median_bpm,
    play_count,
    min_track_bpm,
    artist_genre,
    artist_debut_year,
    track_count,
    track_count_2,
    avg_track_duration_sec,
    CONCAT_WS(': ', title, artist_genre) AS title_artist_genre_label,
    CONCAT_WS('|', title_ref_2, title) AS title_ref_2_title_label
FROM profiled_2
ORDER BY play_count DESC
