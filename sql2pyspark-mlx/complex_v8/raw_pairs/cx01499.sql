WITH cleaned_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        ADD_MONTHS(release_date, 12) AS release_date_plus_12m,
        ROUND(duration_sec / NULLIF(bpm, 0), 3) AS duration_sec_per_bpm
    FROM tracks
    WHERE UPPER(TRIM(title)) LIKE 'G%'
      AND release_date IS NOT NULL
),
latest AS (
    -- Keep the earliest row per artist_id
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        release_date_plus_12m,
        duration_sec_per_bpm
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY release_date, id) AS rn
        FROM cleaned_tracks
    ) ranked
    WHERE rn = 1
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM plays p WHERE p.track_id = s.id) AS play_count,
        (SELECT COUNT(CASE WHEN p.is_skipped = TRUE THEN 1 END) FROM plays p WHERE p.track_id = s.id) AS play_is_skipped_count
    FROM latest s
)
-- Final projection
SELECT
    id,
    artist_id,
    title,
    duration_sec,
    release_date,
    is_explicit,
    moods,
    release_date_plus_12m,
    duration_sec_per_bpm,
    play_count,
    play_is_skipped_count,
    DATE_FORMAT(release_date_plus_12m, 'MMM yyyy') AS release_date_plus_12m_label,
    ROUND(play_count / NULLIF(bpm, 0), 3) AS play_count_per_bpm
FROM summarized_rows
ORDER BY release_date
