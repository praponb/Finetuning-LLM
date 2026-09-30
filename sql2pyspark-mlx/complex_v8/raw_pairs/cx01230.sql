WITH base_artists AS (
    -- Filter artists
    SELECT
        id,
        artist_name,
        genre,
        country,
        debut_year,
        is_verified,
        COALESCE(debut_year, 0) AS debut_year_filled,
        SUBSTRING(TRIM(artist_name), 1, 2) AS artist_name_prefix
    FROM artists
    WHERE genre NOT IN ('Classical', 'Indie')
),
without_matches AS (
    -- Compare against the overall min
    SELECT *
    FROM base_artists
    WHERE debut_year > (SELECT MIN(debut_year) FROM base_artists)
),
screened AS (
    -- Rows that have matching tracks
    SELECT s.*
    FROM without_matches s
    WHERE s.id IN (
            SELECT t.artist_id
            FROM tracks t
            WHERE t.is_explicit = TRUE
        )
      AND s.debut_year <= 2018
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        artist_name,
        country,
        debut_year,
        is_verified,
        debut_year_filled,
        DENSE_RANK() OVER (PARTITION BY country, genre ORDER BY debut_year_filled) AS debut_year_filled_dense_rank,
        ROW_NUMBER() OVER (PARTITION BY country, genre ORDER BY debut_year_filled, id) AS seq_no,
        LAG(debut_year_filled, 3) OVER w_country_genre_by_debut_year_2 AS prev3_debut_year_filled,
        ROUND(debut_year - AVG(debut_year) OVER w_is_verified, 2) AS debut_year_vs_is_verified_avg
    FROM screened
    WINDOW w_country_genre_by_debut_year_2 AS (PARTITION BY country, genre ORDER BY debut_year_filled, id), w_is_verified AS (PARTITION BY is_verified)
)
SELECT
    id,
    artist_name,
    country,
    debut_year,
    is_verified,
    debut_year_filled,
    debut_year_filled_dense_rank,
    seq_no,
    prev3_debut_year_filled,
    debut_year_vs_is_verified_avg,
    prev3_debut_year_filled - seq_no AS prev3_debut_year_filled_minus_seq_no
FROM sequenced
WHERE UPPER(TRIM(artist_name)) LIKE 'P%'
