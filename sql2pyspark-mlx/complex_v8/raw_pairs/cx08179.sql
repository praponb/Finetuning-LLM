WITH recent AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        release_date,
        is_explicit,
        moods,
        DATE_TRUNC('MONTH', release_date) AS release_date_month
    FROM tracks
),
enriched AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.release_date,
        s.is_explicit,
        s.moods,
        s.release_date_month,
        a.country AS artist_country,
        a.genre AS artist_genre,
        a.artist_name
    FROM recent s
    JOIN artists a ON a.id = s.artist_id
)
SELECT
    id,
    artist_id,
    release_date,
    is_explicit,
    release_date_month,
    artist_genre,
    DATE_TRUNC('YEAR', release_date_month) AS release_date_month_year
FROM enriched
WHERE LOWER(artist_name) LIKE '%te%'
ORDER BY artist_genre DESC
