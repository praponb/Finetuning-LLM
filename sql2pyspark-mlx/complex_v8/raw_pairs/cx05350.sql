WITH prepared_artists AS (
    -- Scope the artists rows
    SELECT
        id,
        genre,
        country,
        debut_year
    FROM artists
    WHERE country = 'US'
      AND genre = 'Classical'
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        RANK() OVER (PARTITION BY country, genre ORDER BY debut_year DESC) AS debut_year_rank,
        COUNT(*) OVER (PARTITION BY genre) AS rows_in_genre,
        ROUND(debut_year / NULLIF(SUM(debut_year) OVER (PARTITION BY country, genre), 0), 4) AS debut_year_share_of_country_genre,
        ROUND(AVG(debut_year) OVER (PARTITION BY genre), 2) AS genre_avg_debut_year,
        LAG(debut_year) OVER w_country_by_debut_year AS prev_debut_year,
        ROUND(PERCENT_RANK() OVER (PARTITION BY genre, country ORDER BY debut_year, id), 3) AS debut_year_percent_rank,
        FIRST_VALUE(debut_year) IGNORE NULLS OVER (PARTITION BY country, genre ORDER BY debut_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_debut_year,
        RANK() OVER (ORDER BY debut_year DESC NULLS FIRST, id) AS debut_year_rank_2,
        LEAD(debut_year, 2) OVER w_country_genre_by_debut_year_3 AS next2_debut_year
    FROM prepared_artists
    WINDOW w_country_by_debut_year AS (PARTITION BY country ORDER BY debut_year ASC, id), w_country_genre_by_debut_year_3 AS (PARTITION BY country, genre ORDER BY debut_year DESC, id)
),
top_per_group AS (
    -- First row per group
    SELECT *
    FROM sequenced
    WHERE debut_year_rank = 1
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        genre,
        country,
        debut_year,
        debut_year_rank,
        rows_in_genre,
        genre_avg_debut_year,
        prev_debut_year,
        debut_year_percent_rank,
        first_known_debut_year,
        debut_year_rank_2,
        next2_debut_year,
        LEAD(first_known_debut_year, 2, 0) OVER w_debut_year_rank_2_genre_by_d AS next2_first_known_debut_year,
        prev_debut_year - LAG(prev_debut_year) OVER (PARTITION BY country, debut_year_rank_2 ORDER BY next2_debut_year, id) AS prev_debut_year_change,
        LAST_VALUE(prev_debut_year) IGNORE NULLS OVER (PARTITION BY debut_year_rank ORDER BY genre_avg_debut_year ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_prev_debut_year
    FROM top_per_group
    WINDOW w_debut_year_rank_2_genre_by_d AS (PARTITION BY debut_year_rank_2, genre ORDER BY debut_year_rank_2 ASC, id)
)
SELECT
    id,
    genre,
    debut_year_rank,
    rows_in_genre,
    genre_avg_debut_year,
    prev_debut_year,
    debut_year_percent_rank,
    debut_year_rank_2,
    next2_debut_year,
    next2_first_known_debut_year,
    last_known_prev_debut_year,
    INITCAP(TRIM(genre)) AS genre_clean
FROM ranked
WHERE prev_debut_year_change < 1214
ORDER BY debut_year_rank, debut_year_percent_rank DESC NULLS FIRST
