WITH prepared AS (
    -- Scope the artists rows
    SELECT
        id,
        artist_name,
        genre,
        country,
        debut_year,
        is_verified,
        IF(is_verified, 'yes', 'no') AS is_verified_label
    FROM artists
    WHERE genre = 'HipHop'
),
with_activity AS (
    -- Compare against the overall min
    SELECT *
    FROM prepared
    WHERE debut_year > (SELECT MIN(debut_year) FROM prepared)
),
genre_stats AS (
    -- Max debut_year per genre
    SELECT genre, MAX(debut_year) AS max_debut_year
    FROM with_activity
    GROUP BY genre
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_debut_year,
        ROUND(s.debut_year - g.max_debut_year, 2) AS debut_year_minus_max
    FROM with_activity s
    JOIN genre_stats g ON g.genre = s.genre
    WHERE s.debut_year = g.max_debut_year
)
SELECT
    id,
    country,
    debut_year,
    is_verified,
    is_verified_label,
    max_debut_year,
    debut_year_minus_max,
    CONCAT(artist_name, '#', CAST(id AS STRING)) AS artist_name_ref
FROM outliers
WHERE debut_year_minus_max > -1011.5
ORDER BY id DESC, artist_name_ref
