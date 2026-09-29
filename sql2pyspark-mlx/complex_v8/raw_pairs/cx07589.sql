WITH prepared_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        moods,
        GREATEST(duration_sec, bpm) AS greatest_duration_sec_bpm,
        INITCAP(TRIM(title)) AS title_clean
    FROM tracks
    WHERE (bpm BETWEEN 85 AND 147 OR duration_sec >= 164)
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        duration_sec,
        release_date,
        moods,
        greatest_duration_sec_bpm,
        title_clean,
        CONCAT(title_clean, '#', CAST(id AS STRING)) AS title_clean_ref
    FROM prepared_tracks
)
-- Final projection
SELECT
    id,
    duration_sec,
    release_date,
    moods,
    greatest_duration_sec_bpm
FROM enriched
WHERE duration_sec > 165
ORDER BY duration_sec NULLS LAST
