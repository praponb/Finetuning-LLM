WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM artists
    WHERE debut_year <= 2016
),
stats AS (
    -- Group-level totals
    SELECT
        genre,
        is_verified,
        country,
        ROUND(STDDEV(debut_year), 2) AS stddev_debut_year,
        COUNT(*) FILTER (WHERE country NOT IN ('JP', 'KR')) AS n_country_not_in_jp_kr,
        COUNT(*) AS n_rows,
        ARRAY_SORT(COLLECT_SET(country)) AS country_list,
        PERCENTILE_APPROX(debut_year, 0.5) AS median_debut_year
    FROM valid
    GROUP BY 1, 2, 3
),
screened AS (
    -- Compare against the overall min
    SELECT *
    FROM stats
    WHERE median_debut_year > (SELECT MIN(median_debut_year) FROM stats)
),
qualified AS (
    -- Compare against the overall max
    SELECT *
    FROM screened
    WHERE stddev_debut_year < (SELECT MAX(stddev_debut_year) FROM screened)
),
screened_2 AS (
    -- Compare against the overall avg
    SELECT *
    FROM qualified
    WHERE n_rows > (SELECT AVG(n_rows) FROM qualified)
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        genre,
        is_verified,
        country,
        stddev_debut_year,
        n_rows,
        country_list,
        median_debut_year,
        ROUND(stddev_debut_year - AVG(stddev_debut_year) OVER w_is_verified, 2) AS stddev_debut_year_vs_is_verified_avg,
        FIRST_VALUE(stddev_debut_year) IGNORE NULLS OVER (PARTITION BY is_verified, genre ORDER BY n_country_not_in_jp_kr ASC, genre, is_verified, country ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_stddev_debut_year
    FROM screened_2
    WINDOW w_is_verified AS (PARTITION BY is_verified)
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        LAG(n_rows, 1, 0) OVER (PARTITION BY n_rows, country ORDER BY stddev_debut_year ASC, genre, is_verified, country) AS prev_n_rows,
        DENSE_RANK() OVER (PARTITION BY n_rows, country ORDER BY stddev_debut_year ASC) AS stddev_debut_year_dense_rank
    FROM windowed
)
SELECT
    genre,
    is_verified,
    stddev_debut_year,
    country_list,
    median_debut_year,
    first_known_stddev_debut_year,
    prev_n_rows,
    stddev_debut_year_dense_rank,
    SIZE(country_list) AS country_list_count
FROM with_windows
ORDER BY first_known_stddev_debut_year, prev_n_rows DESC NULLS FIRST
