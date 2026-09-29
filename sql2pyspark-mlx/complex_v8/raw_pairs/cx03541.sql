WITH scoped_artists AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_name,
        genre,
        country,
        debut_year,
        is_verified,
        SUBSTRING(TRIM(artist_name), 1, 3) AS artist_name_prefix
    FROM artists
    WHERE country IN ('US', 'JP', 'TH')
      AND is_verified = FALSE
),
all_events AS (
    -- Stack the branches
    SELECT country, genre AS category
    FROM scoped_artists
    UNION ALL
    SELECT artist_name, genre
    FROM artists
    WHERE is_verified = TRUE
      AND country IS NOT NULL
    UNION ALL
    SELECT device, source
    FROM plays
    WHERE track_id IS NOT NULL
      AND ms_played <= 324058
)
SELECT country, COUNT(*) AS n_rows
FROM all_events
GROUP BY country
ORDER BY country
