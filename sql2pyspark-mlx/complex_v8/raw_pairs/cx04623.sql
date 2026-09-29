WITH scoped AS (
    -- Filter artists
    SELECT
        id,
        artist_name,
        country,
        debut_year,
        is_verified,
        SUBSTRING(TRIM(genre), 1, 2) AS genre_prefix
    FROM artists
    WHERE (debut_year BETWEEN 1994 AND 2015 OR UPPER(TRIM(artist_name)) LIKE 'S%')
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(debut_year - AVG(debut_year) OVER w_is_verified, 2) AS debut_year_vs_is_verified_avg,
        DENSE_RANK() OVER (PARTITION BY is_verified ORDER BY debut_year) AS debut_year_dense_rank,
        SUM(debut_year) OVER (PARTITION BY is_verified ORDER BY debut_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_debut_year,
        LAG(debut_year) OVER (PARTITION BY is_verified ORDER BY debut_year, id) AS prev_debut_year
    FROM scoped
    WINDOW w_is_verified AS (PARTITION BY is_verified)
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(prev_debut_year, 0) AS prev_debut_year_filled,
        ROUND(prev_debut_year / NULLIF(debut_year, 0), 3) AS prev_debut_year_per_debut_year
    FROM metrics
    WHERE running_debut_year < 14758
),
without_matches AS (
    -- Rows that have matching tracks
    SELECT s.*
    FROM labeled s
    WHERE s.id IN (
            SELECT t.artist_id
            FROM tracks t
        )
),
country_stats AS (
    -- Median running_debut_year per country
    SELECT country, PERCENTILE_APPROX(running_debut_year, 0.5) AS median_running_debut_year
    FROM without_matches
    GROUP BY country
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_running_debut_year,
        ROUND(s.running_debut_year - g.median_running_debut_year, 2) AS running_debut_year_minus_median
    FROM without_matches s
    JOIN country_stats g ON g.country = s.country
    WHERE s.running_debut_year >= g.median_running_debut_year
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        GREATEST(debut_year, prev_debut_year_filled) AS greatest_debut_year_prev_debut_year_filled
    FROM compared
    WHERE running_debut_year_minus_median < 8092.0
)
SELECT
    id,
    artist_name,
    debut_year,
    is_verified,
    debut_year_dense_rank,
    prev_debut_year,
    prev_debut_year_per_debut_year,
    running_debut_year_minus_median,
    greatest_debut_year_prev_debut_year_filled,
    ABS(prev_debut_year_filled - 2003) AS prev_debut_year_filled_distance,
    CONCAT_WS(' / ', genre_prefix, country) AS genre_prefix_country_label
FROM scored
ORDER BY running_debut_year_minus_median DESC, prev_debut_year_per_debut_year
