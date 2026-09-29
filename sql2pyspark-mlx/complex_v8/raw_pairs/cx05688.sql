WITH recent AS (
    -- Scope the artists rows
    SELECT
        id,
        artist_name,
        genre,
        country,
        debut_year,
        is_verified,
        CASE WHEN is_verified THEN 1 ELSE 0 END AS is_verified_flag
    FROM artists
    WHERE country IN ('BR', 'US', 'TH')
      AND artist_name IS NOT NULL
),
qualified AS (
    -- Rows that have matching tracks
    SELECT s.*
    FROM recent s
    WHERE EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.id
              AND t.is_explicit = FALSE
        )
      AND s.country IN ('JP', 'BR')
),
unique_rows AS (
    -- Keep the latest row per country, genre
    SELECT
        id,
        artist_name,
        genre,
        country,
        debut_year,
        is_verified,
        is_verified_flag
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY country, genre ORDER BY is_verified_flag DESC, id DESC) AS recency_rank
        FROM qualified
    ) ranked
    WHERE recency_rank = 1
),
scored AS (
    -- Label and score rows
    SELECT *, CONCAT(genre, '#', CAST(debut_year AS STRING)) AS genre_ref
    FROM unique_rows
)
SELECT
    is_verified,
    genre,
    is_verified_flag,
    GROUPING(is_verified) AS is_total_is_verified,
    ROUND(AVG(is_verified_flag), 2) AS avg_is_verified_flag
FROM scored
GROUP BY CUBE(is_verified, genre, is_verified_flag)
