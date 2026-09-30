WITH scoped AS (
    -- Scope the artists rows
    SELECT
        id,
        genre,
        country,
        debut_year,
        is_verified
    FROM artists
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        genre,
        debut_year,
        is_verified,
        FIRST_VALUE(debut_year) OVER (PARTITION BY is_verified ORDER BY debut_year ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_debut_year,
        COUNT(*) OVER w_genre AS rows_in_genre,
        NTILE(3) OVER (PARTITION BY country ORDER BY debut_year, id) AS debut_year_tercile,
        ROUND(CUME_DIST() OVER w_country_by_debut_year_ties, 3) AS debut_year_cume_dist,
        RANK() OVER w_country_by_debut_year_ties AS debut_year_rank
    FROM scoped
    WINDOW w_genre AS (PARTITION BY genre), w_country_by_debut_year_ties AS (PARTITION BY country ORDER BY debut_year)
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        LENGTH(TRIM(genre)) AS genre_len,
        ROUND(debut_year_cume_dist / NULLIF(debut_year_tercile, 0), 3) AS debut_year_cume_dist_per_debut_year_tercile,
        LOWER(REGEXP_REPLACE(TRIM(genre), ' ', '_')) AS genre_slug
    FROM ranked
),
tagged AS (
    -- Add calculated columns
    SELECT *, LOWER(TRIM(genre_slug)) AS genre_slug_clean
    FROM enriched
    WHERE rows_in_genre BETWEEN 6 AND 19
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        ABS(debut_year_tercile - 4) AS debut_year_tercile_distance,
        ROUND(debut_year / NULLIF(debut_year_cume_dist, 0), 3) AS debut_year_per_debut_year_cume_dist
    FROM tagged
),
with_activity AS (
    -- Rows that have matching tracks
    SELECT s.*
    FROM annotated s
    WHERE EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.id
              AND t.is_explicit = FALSE
              AND UPPER(TRIM(t.title)) LIKE 'R%'
        )
),
genre_stats AS (
    -- Median first_debut_year per genre
    SELECT genre, PERCENTILE_APPROX(first_debut_year, 0.5) AS median_first_debut_year
    FROM with_activity
    GROUP BY genre
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_first_debut_year,
        ROUND(s.first_debut_year - g.median_first_debut_year, 2) AS first_debut_year_minus_median
    FROM with_activity s
    JOIN genre_stats g ON g.genre = s.genre
    WHERE s.first_debut_year >= g.median_first_debut_year
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT genre_len, debut_year_tercile, genre, debut_year_rank
    FROM above_average
) src
PIVOT (
    MAX(debut_year_rank)
    FOR genre IN ('Indie', 'Classical', 'Jazz')
)
ORDER BY genre_len, debut_year_tercile
