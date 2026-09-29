WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        moods,
        YEAR(release_date) AS release_date_year,
        DATEDIFF(DATE '2025-06-30', release_date) AS days_since_release_date,
        LENGTH(TRIM(title)) AS title_len
    FROM tracks
    WHERE NOT ARRAY_CONTAINS(moods, 'chill')
),
per_group AS (
    -- Summarise per group
    SELECT
        title_len,
        artist_id,
        COUNT(DISTINCT artist_id) AS distinct_artist_ids,
        ROUND(SUM(release_date_year) / NULLIF(SUM(days_since_release_date), 0), 3) AS release_date_year_to_days_since_release_date_rat,
        ROUND(SUM(days_since_release_date) / NULLIF(SUM(duration_sec), 0), 3) AS days_since_release_date_to_duration_sec_ratio
    FROM filtered
    WHERE duration_sec < 365
    GROUP BY title_len, artist_id
),
scored AS (
    -- Label and score rows
    SELECT
        title_len,
        artist_id,
        release_date_year_to_days_since_release_date_rat,
        days_since_release_date_to_duration_sec_ratio,
        title_len - distinct_artist_ids AS title_len_minus_distinct_artist_ids,
        COALESCE(distinct_artist_ids, 0) AS distinct_artist_ids_filled
    FROM per_group
),
line_level_tracks AS (
    -- One row per track
    SELECT
        s.title_len,
        s.artist_id,
        s.release_date_year_to_days_since_release_date_rat,
        s.days_since_release_date_to_duration_sec_ratio,
        t.id AS track_id,
        t.release_date AS track_release_date,
        t.title AS track_title,
        t.bpm AS track_bpm
    FROM scored s
    JOIN tracks t ON t.artist_id = s.artist_id
)
-- Native SQL UNPIVOT (wide to long)
SELECT title_len, artist_id, measure, value
FROM (
    SELECT title_len, artist_id, release_date_year_to_days_since_release_date_rat, days_since_release_date_to_duration_sec_ratio
    FROM line_level_tracks
) src
UNPIVOT (
    value FOR measure IN (release_date_year_to_days_since_release_date_rat, days_since_release_date_to_duration_sec_ratio)
) unp
ORDER BY title_len, artist_id, measure
