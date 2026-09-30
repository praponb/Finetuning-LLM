WITH prepared AS (
    -- Scope the artists rows
    SELECT
        id,
        artist_name,
        genre,
        country,
        debut_year,
        is_verified
    FROM artists
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LEAD(debut_year) OVER (ORDER BY debut_year DESC, id) AS next_debut_year,
        LAST_VALUE(debut_year) OVER (ORDER BY debut_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_debut_year,
        ROUND(debut_year - AVG(debut_year) OVER (PARTITION BY country, genre), 2) AS debut_year_vs_country_genre_avg,
        DENSE_RANK() OVER (ORDER BY debut_year DESC) AS debut_year_dense_rank,
        MAX(debut_year) OVER (PARTITION BY country, genre) AS country_genre_max_debut_year
    FROM prepared
),
stats AS (
    -- Group-level totals
    SELECT
        country,
        is_verified,
        debut_year_dense_rank,
        COUNT(DISTINCT country) AS distinct_countrys,
        ROUND(AVG(country_genre_max_debut_year), 2) AS avg_country_genre_max_debut_year,
        COUNT(*) AS n_rows,
        SUM(next_debut_year) AS total_next_debut_year
    FROM sequenced
    WHERE NOT is_verified
    GROUP BY country, is_verified, debut_year_dense_rank
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(distinct_countrys) OVER (PARTITION BY debut_year_dense_rank, n_rows), 2) AS debut_year_dense_rank_n_rows_avg_distinct_countr,
        RANK() OVER w_debut_year_dense_rank_by_n_r AS n_rows_rank,
        ROUND(AVG(debut_year_dense_rank) OVER w_debut_year_dense_rank_n_rows, 2) AS debut_year_dense_rank_n_rows_avg_debut_year_dens,
        distinct_countrys - LAG(distinct_countrys) OVER (PARTITION BY debut_year_dense_rank ORDER BY n_rows NULLS LAST, country, is_verified, debut_year_dense_rank) AS distinct_countrys_change,
        MAX(n_rows) OVER (PARTITION BY debut_year_dense_rank, n_rows) AS debut_year_dense_rank_n_rows_max_n_rows,
        ROUND(AVG(total_next_debut_year) OVER (PARTITION BY debut_year_dense_rank, n_rows), 2) AS debut_year_dense_rank_n_rows_avg_total_next_debu
    FROM stats
    WINDOW w_debut_year_dense_rank_by_n_r AS (PARTITION BY debut_year_dense_rank ORDER BY n_rows NULLS LAST), w_debut_year_dense_rank_n_rows AS (PARTITION BY debut_year_dense_rank, n_rows)
)
SELECT country, is_verified, metric_name, amount
FROM (
    SELECT country, is_verified, debut_year_dense_rank_n_rows_avg_distinct_countr, debut_year_dense_rank_n_rows_avg_debut_year_dens
    FROM ranked
) src
UNPIVOT (
    amount FOR metric_name IN (debut_year_dense_rank_n_rows_avg_distinct_countr, debut_year_dense_rank_n_rows_avg_debut_year_dens)
) unp
