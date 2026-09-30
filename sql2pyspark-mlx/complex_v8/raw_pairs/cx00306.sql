WITH prepared_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        bpm,
        moods,
        LOWER(TRIM(title)) AS title_clean,
        DATEDIFF(DATE '2025-12-31', release_date) AS days_since_release_date,
        QUARTER(release_date) AS release_date_quarter
    FROM tracks
    WHERE artist_id IS NOT NULL
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(days_since_release_date, 0) AS days_since_release_date_filled
    FROM prepared_tracks
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        GREATEST(bpm, days_since_release_date) AS greatest_bpm_days_since_release_date
    FROM annotated
    WHERE LENGTH(TRIM(title_clean)) >= 4
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.is_explicit,
        s.bpm,
        s.title_clean,
        s.days_since_release_date,
        s.release_date_quarter,
        s.days_since_release_date_filled,
        s.greatest_bpm_days_since_release_date,
        a.genre AS artist_genre,
        a.country AS artist_country,
        a.is_verified AS artist_is_verified
    FROM labeled s
    JOIN artists a ON a.id = s.artist_id
)
-- Subtotals with CUBE
SELECT
    artist_country,
    release_date_quarter,
    GROUPING(artist_country) AS is_total_artist_country,
    ROUND(SUM(greatest_bpm_days_since_release_date) / NULLIF(SUM(days_since_release_date_filled), 0), 3) AS greatest_bpm_days_since_release_date_to_days_sin,
    ARRAY_SORT(COLLECT_SET(artist_genre)) AS artist_genre_list,
    SUM(duration_sec) AS total_duration_sec
FROM enriched
GROUP BY CUBE(artist_country, release_date_quarter)
ORDER BY artist_country NULLS LAST, release_date_quarter NULLS LAST
