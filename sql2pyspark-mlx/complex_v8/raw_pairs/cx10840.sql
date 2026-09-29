WITH valid_artists AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_name,
        genre,
        debut_year,
        is_verified,
        LENGTH(TRIM(genre)) AS genre_len
    FROM artists
    WHERE NOT is_verified
      AND LENGTH(TRIM(artist_name)) >= 6
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        artist_name,
        debut_year,
        genre_len,
        debut_year - LAG(debut_year) OVER w_genre_len_is_verified_by_deb AS debut_year_change,
        genre_len - LAG(genre_len) OVER (ORDER BY genre_len DESC, id) AS genre_len_change,
        COUNT(*) OVER (PARTITION BY genre, genre_len) AS rows_in_genre_genre_len,
        RANK() OVER w_genre_len_is_verified_by_deb_2 AS debut_year_rank,
        ROUND(AVG(debut_year) OVER (PARTITION BY is_verified, genre_len ORDER BY genre_len DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS debut_year_avg_1_prior,
        ROUND(PERCENT_RANK() OVER (PARTITION BY is_verified, genre_len ORDER BY genre_len DESC), 3) AS genre_len_percent_rank,
        DENSE_RANK() OVER w_is_verified_genre_len_by_gen_2 AS genre_len_dense_rank,
        ROUND(PERCENT_RANK() OVER w_all_by_debut_year_ties, 3) AS debut_year_percent_rank,
        MIN(genre_len) OVER (PARTITION BY is_verified) AS is_verified_min_genre_len
    FROM valid_artists
    WINDOW w_genre_len_is_verified_by_deb AS (PARTITION BY genre_len, is_verified ORDER BY debut_year DESC, id), w_genre_len_is_verified_by_deb_2 AS (PARTITION BY genre_len, is_verified ORDER BY debut_year DESC), w_is_verified_genre_len_by_gen_2 AS (PARTITION BY is_verified, genre_len ORDER BY genre_len DESC), w_all_by_debut_year_ties AS (ORDER BY debut_year)
),
rollup_base AS (
    -- Summarise per group
    SELECT
        genre_len_dense_rank,
        genre_len,
        MAX(debut_year) AS max_debut_year,
        COUNT(*) FILTER (WHERE UPPER(TRIM(artist_name)) LIKE 'K%') AS n_upper_trim_artist_name_like,
        SUM(genre_len_dense_rank) AS total_genre_len_dense_rank
    FROM sequenced
    WHERE debut_year_rank < 9
    GROUP BY genre_len_dense_rank, genre_len
    HAVING MAX(rows_in_genre_genre_len) > 5
)
SELECT
    genre_len_dense_rank,
    genre_len,
    max_debut_year,
    n_upper_trim_artist_name_like,
    COALESCE(genre_len_dense_rank, 0) AS genre_len_dense_rank_filled,
    COALESCE(genre_len, 0) AS genre_len_filled
FROM rollup_base
ORDER BY genre_len_dense_rank_filled DESC
