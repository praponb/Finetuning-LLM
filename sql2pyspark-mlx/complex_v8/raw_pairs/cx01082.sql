WITH cleaned AS (
    -- Scope the artists rows
    SELECT
        id,
        artist_name,
        genre,
        country,
        debut_year,
        is_verified,
        CONCAT_WS('-', genre, artist_name) AS genre_artist_name_label
    FROM artists
),
country_stats AS (
    -- Max debut_year per country
    SELECT country, MAX(debut_year) AS max_debut_year
    FROM cleaned
    GROUP BY country
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_debut_year,
        ROUND(s.debut_year - g.max_debut_year, 2) AS debut_year_minus_max
    FROM cleaned s
    JOIN country_stats g ON g.country = s.country
    WHERE s.debut_year = g.max_debut_year
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        is_verified,
        MAX(max_debut_year) AS max_max_debut_year,
        COUNT(*) AS cnt,
        ROUND(SUM(debut_year) / NULLIF(SUM(max_debut_year), 0), 3) AS debut_year_to_max_debut_year_ratio,
        COUNT(DISTINCT genre) AS distinct_genres,
        ARRAY_SORT(COLLECT_SET(artist_name)) AS artist_name_list
    FROM vs_group
    GROUP BY is_verified
)
SELECT
    is_verified,
    cnt,
    debut_year_to_max_debut_year_ratio,
    distinct_genres,
    CASE WHEN max_max_debut_year >= 2013.1 THEN 'HIGH' WHEN max_max_debut_year >= 2003.2 THEN 'MEDIUM' ELSE 'LOW' END AS max_max_debut_year_tier
FROM summary
