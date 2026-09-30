WITH prepared_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        LOWER(REGEXP_REPLACE(TRIM(title), ' ', '_')) AS title_slug,
        LOWER(TRIM(title)) AS title_clean,
        bpm - duration_sec AS bpm_minus_duration_sec
    FROM tracks
    WHERE UPPER(TRIM(title)) LIKE 'G%'
      AND YEAR(release_date) = 2025
),
filtered AS (
    -- Keep rows whose artist matches
    SELECT s.*
    FROM prepared_tracks s
    WHERE EXISTS (
            SELECT 1
            FROM artists a
            WHERE a.id = s.artist_id
              AND a.genre <> 'HipHop'
              AND NOT a.is_verified
        )
),
annotated AS (
    -- Derive helper columns
    SELECT *, LENGTH(TRIM(title_slug)) AS title_slug_len
    FROM filtered
),
labeled AS (
    -- Derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        moods,
        title_slug,
        title_clean,
        bpm_minus_duration_sec,
        title_slug_len,
        DATE_FORMAT(release_date, 'yyyy-MM-dd') AS release_date_label,
        ROUND(title_slug_len / NULLIF(bpm, 0), 3) AS title_slug_len_per_bpm
    FROM annotated
)
-- Final projection
SELECT
    id,
    title,
    duration_sec,
    is_explicit,
    bpm_minus_duration_sec,
    title_slug_len,
    bpm_minus_duration_sec - title_slug_len AS bpm_minus_duration_sec_minus_title_slug_len
FROM labeled
ORDER BY id DESC, bpm_minus_duration_sec DESC
