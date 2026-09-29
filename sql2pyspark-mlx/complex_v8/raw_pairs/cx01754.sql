WITH scoped_tracks AS (
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
        ROUND(bpm / NULLIF(duration_sec, 0), 3) AS bpm_per_duration_sec,
        DATE_TRUNC('YEAR', release_date) AS release_date_year
    FROM tracks
    WHERE is_explicit = FALSE
),
combined_details AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.bpm,
        s.release_date_year,
        a.artist_name
    FROM scoped_tracks s
    JOIN artists a ON a.id = s.artist_id
    WHERE LOWER(a.artist_name) LIKE '%pe%'
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        LENGTH(TRIM(title)) AS title_len,
        CONCAT_WS('-', title, artist_name) AS title_artist_name_label,
        MONTH(release_date_year) AS release_date_year_month
    FROM combined_details
    WHERE artist_id IS NOT NULL
),
filtered AS (
    -- Rows that have matching tracks
    SELECT s.*
    FROM labeled s
    WHERE EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
        )
      AND s.title_len > 3
),
screened AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM filtered s
    WHERE s.id IN (
            SELECT p.track_id
            FROM plays p
        )
),
with_parent AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.bpm,
        s.release_date_year,
        s.title_artist_name_label,
        s.release_date_year_month,
        a.artist_name AS artist_artist_name
    FROM screened s
    LEFT JOIN artists a ON a.id = s.artist_id
),
without_matches AS (
    -- Rows with no matching tracks
    SELECT s.*
    FROM with_parent s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
              AND t.is_explicit = TRUE
              AND t.release_date BETWEEN DATE '2024-09-26' AND DATE '2025-04-02'
        )
)
-- Final projection
SELECT
    id,
    artist_id,
    bpm,
    title_artist_name_label,
    artist_artist_name
FROM without_matches
WHERE bpm >= 75
ORDER BY artist_id DESC, title_artist_name_label, id
LIMIT 10
