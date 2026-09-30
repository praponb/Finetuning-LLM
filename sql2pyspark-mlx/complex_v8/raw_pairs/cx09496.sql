WITH filtered_artists AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_name,
        genre,
        country,
        debut_year,
        COALESCE(genre, 'UNKNOWN') AS genre_or_unknown,
        UPPER(TRIM(country)) AS country_clean
    FROM artists
    WHERE debut_year <= 2006
),
totals AS (
    -- Group-level totals
    SELECT
        genre,
        SUM(debut_year) AS total_debut_year,
        COUNT(*) AS row_count,
        COUNT(DISTINCT genre) AS distinct_genres,
        SUM(CASE WHEN genre = 'Jazz' THEN 1 ELSE 0 END) AS n_genre_jazz
    FROM filtered_artists
    WHERE genre_or_unknown = 'HipHop'
    GROUP BY genre
),
filtered AS (
    -- Compare against the overall max
    SELECT *
    FROM totals
    WHERE n_genre_jazz < (SELECT MAX(n_genre_jazz) FROM totals)
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT row_count, genre, total_debut_year
    FROM filtered
) src
PIVOT (
    MAX(total_debut_year)
    FOR genre IN ('HipHop', 'Classical', 'Indie', 'Pop')
)
ORDER BY row_count
