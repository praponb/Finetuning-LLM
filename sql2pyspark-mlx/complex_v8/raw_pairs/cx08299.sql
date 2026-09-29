WITH cleaned AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        moods,
        ABS(duration_sec - 352) AS duration_sec_distance
    FROM tracks
    WHERE duration_sec > 213
),
track_stats AS (
    -- Per-artist tracks metrics
    SELECT
        artist_id,
        MAX(duration_sec) AS max_track_duration_sec,
        MIN(release_date) AS first_track_release_date
    FROM tracks
    GROUP BY artist_id
),
summarized_rows AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        t.max_track_duration_sec,
        t.first_track_release_date
    FROM cleaned s
    LEFT JOIN track_stats t ON t.artist_id = s.artist_id
),
latest AS (
    -- Keep the earliest row per artist_id
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        moods,
        duration_sec_distance,
        max_track_duration_sec,
        first_track_release_date
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY duration_sec_distance, id) AS row_num
        FROM summarized_rows
    ) ranked
    WHERE row_num = 1
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        DATEDIFF(DATE '2026-01-01', release_date) AS days_since_release_date,
        CONCAT(title, '#', CAST(id AS STRING)) AS title_ref
    FROM latest
    WHERE duration_sec_distance < 336
)
SELECT
    id,
    title,
    duration_sec,
    release_date,
    moods,
    duration_sec_distance,
    max_track_duration_sec,
    first_track_release_date,
    days_since_release_date,
    title_ref,
    DATE_FORMAT(release_date, 'MMM yyyy') AS release_date_label
FROM tagged
ORDER BY release_date DESC, title DESC NULLS FIRST
