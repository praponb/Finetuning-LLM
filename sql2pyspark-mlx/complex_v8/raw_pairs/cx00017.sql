WITH scoped_artists AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_name,
        genre,
        debut_year,
        is_verified,
        FLOOR(debut_year / 5) * 5 AS debut_year_bucket,
        COALESCE(genre, 'UNKNOWN') AS genre_or_unknown
    FROM artists
    WHERE country = 'BR'
      AND debut_year <= 2018
),
with_activity AS (
    -- Rows with no matching tracks
    SELECT s.*
    FROM scoped_artists s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.id
              AND t.release_date < DATE '2025-04-02'
              AND t.bpm > 89
        )
),
latest_per_key AS (
    -- Keep the latest row per genre_or_unknown, genre
    SELECT
        id,
        artist_name,
        genre,
        debut_year,
        is_verified,
        debut_year_bucket,
        genre_or_unknown
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY genre_or_unknown, genre ORDER BY debut_year_bucket DESC, id DESC) AS rn
        FROM with_activity
    ) ranked
    WHERE rn = 1
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        debut_year - LAG(debut_year) OVER (ORDER BY debut_year_bucket DESC, id) AS debut_year_change,
        FIRST_VALUE(debut_year) OVER (PARTITION BY genre ORDER BY debut_year_bucket ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_debut_year,
        COUNT(*) OVER w_genre AS rows_in_genre,
        DENSE_RANK() OVER (ORDER BY debut_year_bucket DESC) AS debut_year_bucket_dense_rank,
        MAX(debut_year_bucket) OVER (PARTITION BY genre) AS genre_max_debut_year_bucket
    FROM latest_per_key
    WINDOW w_all_by_debut_year_bucket AS (ORDER BY debut_year_bucket DESC, id), w_genre AS (PARTITION BY genre)
)
-- Subtotals with GROUPING SETS
SELECT
    genre_or_unknown,
    debut_year_bucket_dense_rank,
    GROUPING(genre_or_unknown) AS is_total_genre_or_unknown,
    SUM(first_debut_year) AS total_first_debut_year
FROM with_trends
GROUP BY GROUPING SETS ((genre_or_unknown, debut_year_bucket_dense_rank), (genre_or_unknown), ())
ORDER BY genre_or_unknown NULLS LAST, debut_year_bucket_dense_rank NULLS LAST
