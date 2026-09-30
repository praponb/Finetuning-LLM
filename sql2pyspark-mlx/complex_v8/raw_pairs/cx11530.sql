WITH prepared_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        release_date,
        is_explicit,
        bpm,
        moods,
        LOWER(REGEXP_REPLACE(TRIM(title), ' ', '_')) AS title_slug,
        ABS(bpm - 94) AS bpm_distance
    FROM tracks
    WHERE NOT ARRAY_CONTAINS(moods, 'focus')
      AND release_date >= DATE '2024-11-14'
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        artist_id,
        release_date,
        is_explicit,
        bpm,
        title_slug,
        bpm_distance,
        ROUND(bpm_distance / NULLIF(bpm, 0), 3) AS bpm_distance_per_bpm,
        SIZE(moods) AS moods_count
    FROM prepared_tracks
)
SELECT
    artist_id,
    moods_count,
    GROUPING(artist_id) AS is_total_artist_id,
    MIN(moods_count) AS min_moods_count
FROM derived
GROUP BY GROUPING SETS ((artist_id, moods_count), (artist_id), (moods_count), ())
