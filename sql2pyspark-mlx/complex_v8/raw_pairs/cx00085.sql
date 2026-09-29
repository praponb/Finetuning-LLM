WITH prepared_tracks AS (
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
        DATE_FORMAT(release_date, 'MMM yyyy') AS release_date_label,
        LOWER(TRIM(title)) AS title_clean,
        LOWER(TRIM(title)) AS title_clean_2
    FROM tracks
    WHERE (UPPER(TRIM(title)) LIKE 'M%' OR bpm < 170)
),
screened AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM prepared_tracks s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.id
              AND p.source NOT IN ('search', 'album')
        )
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT(title_clean_2, '#', CAST(artist_id AS STRING)) AS title_clean_2_ref,
        LENGTH(TRIM(title_clean_2)) AS title_clean_2_len
    FROM screened
    WHERE ARRAY_CONTAINS(moods, 'sad')
)
-- Subtotals with GROUPING SETS
SELECT
    title_clean_2_len,
    is_explicit,
    artist_id,
    GROUPING(title_clean_2_len) AS is_total_title_clean_2_len,
    SUM(title_clean_2_len) AS total_title_clean_2_len,
    COUNT(*) AS n_rows
FROM derived
GROUP BY GROUPING SETS ((title_clean_2_len, is_explicit, artist_id), (title_clean_2_len))
ORDER BY title_clean_2_len NULLS LAST, is_explicit NULLS LAST, artist_id NULLS LAST
