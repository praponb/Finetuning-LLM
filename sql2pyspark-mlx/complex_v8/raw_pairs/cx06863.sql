WITH filtered_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        release_date,
        bpm,
        moods,
        DATE_TRUNC('MONTH', release_date) AS release_date_month
    FROM tracks
    WHERE artist_id <= 3
      AND is_explicit = TRUE
),
combined_details_artist AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        a.country AS artist_country,
        a.is_verified AS artist_is_verified,
        a.genre AS artist_genre
    FROM filtered_tracks s
    JOIN artists a ON a.id = s.artist_id
)
SELECT *
FROM (
    SELECT release_date, release_date_month, artist_country, bpm
    FROM combined_details_artist
) src
PIVOT (
    SUM(bpm) AS total, MAX(bpm) AS max_v
    FOR artist_country IN ('KR', 'BR')
)
