WITH cleaned AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        bpm,
        bpm - duration_sec AS bpm_minus_duration_sec,
        YEAR(release_date) AS release_date_year,
        CONCAT(title, '#', CAST(bpm AS STRING)) AS title_ref
    FROM tracks
    WHERE is_explicit = TRUE
),
combined_details_artist AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.bpm,
        s.bpm_minus_duration_sec,
        s.release_date_year,
        s.title_ref,
        a.debut_year AS artist_debut_year
    FROM cleaned s
    JOIN artists a ON a.id = s.artist_id AND a.artist_name IS NOT NULL
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        LOWER(TRIM(title)) AS title_clean,
        FLOOR(duration_sec / 50) * 50 AS duration_sec_bucket
    FROM combined_details_artist
    WHERE bpm_minus_duration_sec > 114
),
qualified AS (
    -- Rows with no matching tracks
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
              AND t.is_explicit = FALSE
              AND t.release_date IS NOT NULL
        )
)
SELECT
    artist_id,
    release_date_year,
    COUNT(*) AS n_rows
FROM qualified
GROUP BY CUBE(artist_id, release_date_year)
ORDER BY artist_id NULLS LAST, release_date_year NULLS LAST
