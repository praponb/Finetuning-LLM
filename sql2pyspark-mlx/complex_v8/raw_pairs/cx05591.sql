WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods
    FROM tracks
    WHERE (duration_sec BETWEEN 164 AND 365 OR is_explicit = FALSE)
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        COALESCE(duration_sec, 0) AS duration_sec_filled,
        INITCAP(TRIM(title)) AS title_clean
    FROM cleaned
),
screened AS (
    -- Keep rows whose artist matches
    SELECT s.*
    FROM annotated s
    WHERE NOT EXISTS (
            SELECT 1
            FROM artists a
            WHERE a.id = s.artist_id
              AND a.is_verified = TRUE
              AND a.debut_year BETWEEN 1997 AND 2018
        )
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        LOWER(TRIM(title)) AS title_clean_2,
        LEAST(duration_sec_filled, bpm) AS least_duration_sec_filled_bpm
    FROM screened
    WHERE duration_sec BETWEEN 128 AND 344
)
-- Final projection
SELECT
    id,
    title,
    duration_sec,
    release_date,
    bpm,
    moods,
    duration_sec_filled,
    title_clean_2,
    least_duration_sec_filled_bpm,
    SIZE(moods) AS moods_count,
    FLOOR(duration_sec / 50) * 50 AS duration_sec_bucket
FROM tagged
