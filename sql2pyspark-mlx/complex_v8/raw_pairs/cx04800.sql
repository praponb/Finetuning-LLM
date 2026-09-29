WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_name,
        genre,
        country,
        is_verified,
        CONCAT(genre, '#', CAST(debut_year AS STRING)) AS genre_ref
    FROM artists
    WHERE NOT is_verified
),
with_activity AS (
    -- Rows that have matching tracks
    SELECT s.*
    FROM cleaned s
    WHERE s.id IN (
            SELECT t.artist_id
            FROM tracks t
        )
),
track_stats AS (
    -- Per-artist tracks metrics
    SELECT artist_id, MIN(duration_sec) AS min_track_duration_sec
    FROM tracks
    GROUP BY artist_id
),
with_counts AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        t.min_track_duration_sec
    FROM with_activity s
    LEFT JOIN track_stats t ON t.artist_id = s.id
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(genre_ref), ' ', '_')) AS genre_ref_slug,
        CONCAT(genre_ref, '#', CAST(min_track_duration_sec AS STRING)) AS genre_ref_ref,
        CASE WHEN country IN ('BR', 'US') THEN 'group_a' WHEN country = 'TH' THEN 'group_b' ELSE 'other' END AS country_group
    FROM with_counts
    WHERE artist_name IS NOT NULL
)
SELECT
    id,
    artist_name,
    country,
    is_verified,
    genre_ref,
    genre_ref_slug,
    genre_ref_ref,
    country_group,
    SUBSTRING(TRIM(genre_ref_slug), 1, 2) AS genre_ref_slug_prefix
FROM labeled
ORDER BY country_group DESC, genre_ref_ref
