WITH recent AS (
    -- Filter artists
    SELECT
        id,
        artist_name,
        genre,
        country,
        debut_year,
        is_verified,
        LENGTH(TRIM(artist_name)) AS artist_name_len,
        COALESCE(genre, 'UNKNOWN') AS genre_or_unknown
    FROM artists
    WHERE genre IS NOT NULL
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        FIRST_VALUE(artist_name_len) IGNORE NULLS OVER (ORDER BY debut_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_artist_name_len,
        LAST_VALUE(artist_name_len) OVER (ORDER BY debut_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_artist_name_len,
        ROUND(CUME_DIST() OVER (PARTITION BY is_verified, genre ORDER BY debut_year), 3) AS debut_year_cume_dist,
        FIRST_VALUE(artist_name_len) OVER (PARTITION BY genre_or_unknown, country ORDER BY artist_name_len ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_artist_name_len,
        ROUND(PERCENT_RANK() OVER (PARTITION BY genre_or_unknown, country ORDER BY artist_name_len ASC), 3) AS artist_name_len_percent_rank,
        SUM(artist_name_len) OVER w_genre_or_unknown AS genre_or_unknown_sum_artist_name_len,
        COUNT(*) OVER (PARTITION BY genre, genre_or_unknown) AS rows_in_genre_genre_or_unknown,
        DENSE_RANK() OVER (ORDER BY debut_year) AS debut_year_dense_rank,
        ROUND(AVG(debut_year) OVER (PARTITION BY genre_or_unknown, country ORDER BY artist_name_len ASC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS debut_year_avg_2,
        LAST_VALUE(debut_year) OVER (PARTITION BY artist_name_len ORDER BY artist_name_len DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_debut_year,
        NTH_VALUE(artist_name_len, 2) OVER (ORDER BY debut_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_artist_name_len,
        ROW_NUMBER() OVER w_all_by_debut_year AS debut_year_seq,
        COUNT(*) OVER (PARTITION BY country) AS rows_in_country
    FROM recent
    WINDOW w_genre_or_unknown AS (PARTITION BY genre_or_unknown), w_all_by_debut_year AS (ORDER BY debut_year, id)
)
-- Final projection
SELECT
    id,
    genre,
    country,
    debut_year,
    is_verified,
    artist_name_len,
    genre_or_unknown,
    first_known_artist_name_len,
    last_artist_name_len,
    debut_year_cume_dist,
    first_artist_name_len,
    artist_name_len_percent_rank,
    genre_or_unknown_sum_artist_name_len,
    rows_in_genre_genre_or_unknown,
    debut_year_dense_rank,
    last_debut_year,
    second_artist_name_len,
    debut_year_seq,
    rows_in_country,
    LEAST(debut_year_avg_2, first_artist_name_len) AS least_debut_year_avg_2_first_artist_name_len
FROM metrics
ORDER BY artist_name_len_percent_rank, first_known_artist_name_len DESC
