WITH filtered_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        bpm,
        moods,
        LENGTH(TRIM(title)) AS title_len
    FROM tracks
    WHERE artist_id IS NOT NULL
      AND SIZE(moods) >= 2
),
with_parent AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.release_date,
        s.bpm,
        s.moods,
        s.title_len,
        a.debut_year AS artist_debut_year,
        a.genre AS artist_genre,
        a.country AS artist_country
    FROM filtered_tracks s
    JOIN artists a ON a.id = s.artist_id
),
filtered AS (
    -- Keep rows whose artist matches
    SELECT s.*
    FROM with_parent s
    WHERE EXISTS (
            SELECT 1
            FROM artists a
            WHERE a.id = s.artist_id
              AND a.country IS NOT NULL
              AND a.is_verified = TRUE
        )
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        ARRAY_JOIN(ARRAY_SORT(moods), ',') AS moods_text,
        CASE WHEN artist_genre IN ('HipHop', 'Classical') THEN 'group_a' WHEN artist_genre = 'Indie' THEN 'group_b' ELSE 'other' END AS artist_genre_group
    FROM filtered
),
track_stats AS (
    -- Per-artist tracks metrics
    SELECT
        artist_id,
        COUNT(*) FILTER (WHERE NOT is_explicit) AS track_is_explicit_count
    FROM tracks
    GROUP BY artist_id
),
summarized_rows AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        COALESCE(t.track_is_explicit_count, 0) AS track_is_explicit_count
    FROM enriched s
    LEFT JOIN track_stats t ON t.artist_id = s.artist_id
),
track_stats_2 AS (
    -- Per-artist tracks metrics
    SELECT
        artist_id,
        MAX(bpm) AS max_track_bpm,
        ROUND(AVG(bpm), 2) AS avg_track_bpm,
        COUNT(CASE WHEN NOT is_explicit THEN 1 END) AS track_is_explicit_count_2
    FROM tracks
    GROUP BY artist_id
),
profiled AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        t.max_track_bpm,
        t.avg_track_bpm,
        COALESCE(t.track_is_explicit_count_2, 0) AS track_is_explicit_count_2
    FROM summarized_rows s
    LEFT JOIN track_stats_2 t ON t.artist_id = s.artist_id
),
detail_rows_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.moods,
        s.title_len,
        s.artist_debut_year,
        s.artist_genre,
        s.artist_genre_group,
        s.max_track_bpm,
        s.avg_track_bpm,
        p.id AS play_id,
        p.listener_id AS play_listener_id,
        p.device AS play_device,
        p.source AS play_source,
        p.is_skipped AS play_is_skipped
    FROM profiled s
    JOIN plays p ON p.track_id = s.id
)
-- Subtotals with GROUPING SETS
SELECT
    play_is_skipped,
    title_len,
    GROUPING_ID() AS gid,
    MIN(artist_debut_year) AS min_artist_debut_year,
    PERCENTILE_APPROX(max_track_bpm, 0.5) AS median_max_track_bpm,
    ARRAY_SORT(COLLECT_SET(play_device)) AS play_device_list
FROM detail_rows_plays
GROUP BY GROUPING SETS ((play_is_skipped, title_len), (play_is_skipped), ())
ORDER BY play_is_skipped NULLS LAST, title_len NULLS LAST
