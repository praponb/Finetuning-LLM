WITH scoped_artists AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_name,
        genre,
        country,
        debut_year,
        is_verified,
        FLOOR(debut_year / 5) * 5 AS debut_year_bucket
    FROM artists
    WHERE LOWER(artist_name) LIKE '%ar%'
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        artist_name,
        genre,
        country,
        debut_year,
        SUM(debut_year) OVER (PARTITION BY genre, country ORDER BY debut_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_debut_year,
        ROUND(debut_year / NULLIF(SUM(debut_year) OVER (PARTITION BY is_verified), 0), 4) AS debut_year_share_of_is_verified,
        SUM(debut_year) OVER (PARTITION BY genre, country ORDER BY debut_year, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS debut_year_sum_6_prior,
        COUNT(*) OVER (PARTITION BY is_verified) AS rows_in_is_verified,
        ROW_NUMBER() OVER w_genre_by_debut_year AS debut_year_seq,
        ROUND(AVG(debut_year_bucket) OVER (PARTITION BY is_verified), 2) AS is_verified_avg_debut_year_bucket,
        MIN(debut_year) OVER (PARTITION BY country, genre) AS country_genre_min_debut_year,
        ROW_NUMBER() OVER (PARTITION BY is_verified, genre ORDER BY debut_year ASC, id) AS rn,
        LEAD(debut_year_bucket, 3) OVER (ORDER BY debut_year_bucket ASC, id) AS next3_debut_year_bucket,
        ROUND(debut_year - AVG(debut_year) OVER w_is_verified, 2) AS debut_year_vs_is_verified_avg,
        ROUND(CUME_DIST() OVER (PARTITION BY is_verified, genre ORDER BY debut_year ASC), 3) AS debut_year_cume_dist
    FROM scoped_artists
    WINDOW w_genre_by_debut_year AS (PARTITION BY genre ORDER BY debut_year, id), w_is_verified AS (PARTITION BY is_verified)
),
leaders AS (
    -- Top 5 per group
    SELECT
        id,
        artist_name,
        genre,
        country,
        debut_year,
        running_debut_year,
        debut_year_share_of_is_verified,
        debut_year_sum_6_prior,
        rows_in_is_verified,
        debut_year_seq,
        is_verified_avg_debut_year_bucket,
        country_genre_min_debut_year,
        next3_debut_year_bucket,
        debut_year_vs_is_verified_avg,
        debut_year_cume_dist
    FROM sequenced
    WHERE rn <= 5
      AND debut_year_seq BETWEEN 4 AND 9
)
SELECT
    id,
    artist_name,
    genre,
    debut_year,
    running_debut_year,
    debut_year_share_of_is_verified,
    rows_in_is_verified,
    debut_year_seq,
    is_verified_avg_debut_year_bucket,
    next3_debut_year_bucket,
    debut_year_vs_is_verified_avg,
    debut_year_cume_dist
FROM leaders
WHERE next3_debut_year_bucket > 1993
ORDER BY artist_name DESC, running_debut_year DESC
