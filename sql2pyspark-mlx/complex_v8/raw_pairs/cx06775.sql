WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_name,
        genre,
        country,
        debut_year,
        LENGTH(TRIM(country)) AS country_len,
        COALESCE(genre, 'UNKNOWN') AS genre_or_unknown,
        ABS(debut_year - 2006) AS debut_year_distance
    FROM artists
),
line_level_tracks AS (
    -- One row per track
    SELECT
        s.id,
        s.artist_name,
        s.country,
        s.country_len,
        t.id AS track_id,
        t.bpm AS track_bpm,
        t.duration_sec AS track_duration_sec,
        t.title AS track_title,
        t.release_date AS track_release_date
    FROM recent s
    JOIN tracks t ON t.artist_id = s.id AND t.release_date IS NOT NULL
),
play_stats AS (
    -- Per-track plays metrics
    SELECT track_id, COUNT(*) AS play_count
    FROM plays
    GROUP BY track_id
),
with_counts AS (
    -- Attach plays metrics
    SELECT
        s.*,
        COALESCE(p.play_count, 0) AS play_count
    FROM line_level_tracks s
    LEFT JOIN play_stats p ON p.track_id = s.track_id
),
with_details_track AS (
    -- Join tracks
    SELECT
        s.id,
        s.artist_name,
        s.country,
        s.country_len,
        s.track_id,
        s.track_bpm,
        s.track_title,
        s.track_release_date,
        t.artist_id AS track_artist_id,
        t.moods AS track_moods
    FROM with_counts s
    JOIN tracks t ON t.id = s.track_id
    WHERE ARRAY_CONTAINS(t.moods, 'workout')
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        artist_name,
        track_id,
        track_bpm,
        track_title,
        track_release_date,
        track_artist_id,
        DATE_TRUNC('QUARTER', track_release_date) AS track_release_date_quarter,
        SUBSTRING(TRIM(track_title), 1, 3) AS track_title_prefix,
        ABS(track_bpm - 89) AS track_bpm_distance
    FROM with_details_track
),
with_counts_2 AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        (SELECT MIN(t.release_date) FROM tracks t WHERE t.artist_id = s.id) AS first_track_release_date
    FROM scored s
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY track_id, track_artist_id ORDER BY track_bpm DESC, id, track_id) AS track_bpm_tercile,
        ROUND(track_bpm_distance / NULLIF(SUM(track_bpm_distance) OVER (PARTITION BY DATE_TRUNC('MONTH', first_track_release_date), track_id), 0), 4) AS track_bpm_distance_share_of_first_track_release_,
        LAST_VALUE(track_bpm) OVER (PARTITION BY track_id, track_artist_id ORDER BY track_bpm DESC, id, track_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_track_bpm,
        DENSE_RANK() OVER w_track_id_track_artist_id_by AS track_bpm_dense_rank,
        MIN(track_bpm) OVER (PARTITION BY DATE_TRUNC('MONTH', first_track_release_date), track_id) AS first_track_release_date_month_track_id_min_trac
    FROM with_counts_2
    WINDOW w_track_id_track_artist_id_by AS (PARTITION BY track_id, track_artist_id ORDER BY track_bpm DESC, id, track_id)
)
SELECT
    id,
    track_bpm,
    track_artist_id,
    track_release_date_quarter,
    track_title_prefix,
    track_bpm_distance,
    first_track_release_date,
    track_bpm_tercile,
    track_bpm_distance_share_of_first_track_release_,
    last_track_bpm,
    SUBSTRING(TRIM(artist_name), 1, 3) AS artist_name_prefix
FROM with_windows
WHERE UPPER(TRIM(track_title)) LIKE 'M%'
ORDER BY last_track_bpm, artist_name_prefix
