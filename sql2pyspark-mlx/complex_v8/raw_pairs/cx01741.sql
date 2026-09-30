WITH filtered AS (
    -- Scope the artists rows
    SELECT
        id,
        artist_name,
        genre,
        debut_year,
        FLOOR(debut_year / 5) * 5 AS debut_year_bucket,
        CONCAT_WS(' / ', artist_name, genre) AS artist_name_genre_label,
        CASE WHEN is_verified THEN 1 ELSE 0 END AS is_verified_flag
    FROM artists
    WHERE (country IN ('JP', 'BR') OR is_verified = TRUE)
),
genre_stats AS (
    -- Avg debut_year per genre
    SELECT genre, AVG(debut_year) AS avg_debut_year
    FROM filtered
    GROUP BY genre
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_debut_year,
        ROUND(s.debut_year - g.avg_debut_year, 2) AS debut_year_minus_avg
    FROM filtered s
    JOIN genre_stats g ON g.genre = s.genre
    WHERE s.debut_year > g.avg_debut_year
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        artist_name,
        genre,
        debut_year,
        debut_year_minus_avg,
        COALESCE(debut_year_bucket, 0) AS debut_year_bucket_filled,
        LOWER(TRIM(artist_name)) AS artist_name_clean,
        GREATEST(debut_year, debut_year_minus_avg) AS greatest_debut_year_debut_year_minus_avg
    FROM vs_group
)
-- Final projection
SELECT
    id,
    artist_name,
    genre,
    debut_year,
    debut_year_minus_avg,
    debut_year_bucket_filled
FROM enriched
WHERE debut_year_bucket_filled > 2000
