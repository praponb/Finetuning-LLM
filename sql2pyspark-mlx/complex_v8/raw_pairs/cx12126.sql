WITH base_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        DATEDIFF(DATE '2025-06-30', release_date) AS days_since_release_date,
        LOWER(TRIM(title)) AS title_clean,
        LENGTH(TRIM(title)) AS title_len
    FROM tracks
),
screened AS (
    -- Keep rows whose artist matches
    SELECT s.*
    FROM base_tracks s
    WHERE EXISTS (
            SELECT 1
            FROM artists a
            WHERE a.id = s.artist_id
              AND a.is_verified = FALSE
        )
),
joined_artist AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.release_date,
        s.title_clean,
        a.is_verified AS artist_is_verified,
        a.country AS artist_country
    FROM screened s
    JOIN artists a ON a.id = s.artist_id
    WHERE a.is_verified = TRUE
),
enriched_artist AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.release_date,
        s.artist_is_verified,
        s.artist_country,
        a.artist_name,
        a.country AS artist_country_2
    FROM joined_artist s
    JOIN artists a ON a.id = s.artist_id
)
-- Final projection
SELECT
    id,
    artist_id,
    release_date,
    artist_is_verified,
    artist_name,
    artist_country_2,
    CASE WHEN artist_is_verified THEN 1 ELSE 0 END AS artist_is_verified_flag
FROM enriched_artist
ORDER BY artist_id DESC, artist_country_2
