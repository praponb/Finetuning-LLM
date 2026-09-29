WITH recent AS (
    -- Filter artists
    SELECT
        id,
        artist_name,
        genre,
        country,
        debut_year,
        is_verified,
        CONCAT(artist_name, '#', CAST(debut_year AS STRING)) AS artist_name_ref,
        ABS(debut_year - 2006) AS debut_year_distance
    FROM artists
    WHERE is_verified = FALSE
      AND UPPER(TRIM(artist_name)) LIKE 'L%'
),
qualified AS (
    -- Rows that have matching tracks
    SELECT s.*
    FROM recent s
    WHERE EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.id
        )
),
genre_stats AS (
    -- Max debut_year_distance per genre
    SELECT genre, MAX(debut_year_distance) AS max_debut_year_distance
    FROM qualified
    GROUP BY genre
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_debut_year_distance,
        ROUND(s.debut_year_distance - g.max_debut_year_distance, 2) AS debut_year_distance_minus_max
    FROM qualified s
    JOIN genre_stats g ON g.genre = s.genre
    WHERE s.debut_year_distance = g.max_debut_year_distance
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY is_verified, country ORDER BY debut_year_distance_minus_max DESC, id) AS rn,
        NTILE(3) OVER w_country_is_verified_by_max_d AS max_debut_year_distance_tercile
    FROM vs_group
    WINDOW w_country_is_verified_by_max_d AS (PARTITION BY country, is_verified ORDER BY max_debut_year_distance, id)
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAG(debut_year, 3) OVER w_is_verified_by_debut_year AS prev3_debut_year,
        ROUND(max_debut_year_distance / NULLIF(SUM(max_debut_year_distance) OVER w_genre, 0), 4) AS max_debut_year_distance_share_of_genre,
        MAX(debut_year) OVER (PARTITION BY is_verified ORDER BY debut_year DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS debut_year_max_2,
        DENSE_RANK() OVER w_is_verified_by_debut_year AS debut_year_dense_rank
    FROM windowed
    WINDOW w_is_verified_by_debut_year AS (PARTITION BY is_verified ORDER BY debut_year DESC, id), w_genre AS (PARTITION BY genre)
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER w_debut_year_dense_rank_genre AS rows_in_debut_year_dense_rank_genre,
        ROUND(debut_year_max_2 - AVG(debut_year_max_2) OVER (PARTITION BY debut_year_dense_rank, genre), 2) AS debut_year_max_2_vs_debut_year_dense_rank_genre_
    FROM metrics
    WINDOW w_debut_year_dense_rank_genre AS (PARTITION BY debut_year_dense_rank, genre)
)
-- Final projection
SELECT
    id,
    artist_name,
    genre,
    country,
    debut_year,
    is_verified,
    artist_name_ref,
    debut_year_distance,
    max_debut_year_distance,
    debut_year_distance_minus_max,
    rn,
    max_debut_year_distance_tercile,
    prev3_debut_year,
    max_debut_year_distance_share_of_genre,
    debut_year_max_2,
    debut_year_dense_rank,
    rows_in_debut_year_dense_rank_genre,
    debut_year_max_2_vs_debut_year_dense_rank_genre_,
    LEAST(prev3_debut_year, debut_year_max_2) AS least_prev3_debut_year_debut_year_max_2
FROM with_trends
ORDER BY debut_year_distance_minus_max DESC
