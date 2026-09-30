WITH filtered AS (
    -- Filter artists
    SELECT
        id,
        artist_name,
        genre,
        country,
        debut_year,
        is_verified,
        INITCAP(TRIM(genre)) AS genre_clean,
        CONCAT_WS('|', genre, country) AS genre_country_label
    FROM artists
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(debut_year) OVER (ORDER BY debut_year, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS debut_year_avg_1_prior,
        ROUND(AVG(debut_year) OVER (PARTITION BY is_verified, country), 2) AS is_verified_country_avg_debut_year
    FROM filtered
),
genre_stats AS (
    -- Avg debut_year per genre
    SELECT genre, AVG(debut_year) AS avg_debut_year
    FROM metrics
    GROUP BY genre
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_debut_year,
        ROUND(s.debut_year - g.avg_debut_year, 2) AS debut_year_minus_avg
    FROM metrics s
    JOIN genre_stats g ON g.genre = s.genre
    WHERE s.debut_year > g.avg_debut_year
),
without_matches AS (
    -- Rows with no matching tracks
    SELECT s.*
    FROM above_average s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.id
              AND t.bpm > 100
              AND t.duration_sec <= 344
        )
      AND LOWER(s.genre_clean) LIKE '%az%'
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        artist_name,
        genre,
        country,
        debut_year,
        is_verified,
        genre_clean,
        genre_country_label,
        debut_year_avg_1_prior,
        is_verified_country_avg_debut_year,
        avg_debut_year,
        debut_year_minus_avg,
        FIRST_VALUE(debut_year_avg_1_prior) IGNORE NULLS OVER (PARTITION BY genre ORDER BY is_verified_country_avg_debut_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_debut_year_avg_1_prior,
        NTILE(3) OVER (PARTITION BY genre ORDER BY is_verified_country_avg_debut_year DESC, id) AS is_verified_country_avg_debut_year_tercile,
        MAX(debut_year_minus_avg) OVER (PARTITION BY genre ORDER BY is_verified_country_avg_debut_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_debut_year_minus_avg_so_far,
        ROUND(PERCENT_RANK() OVER (PARTITION BY genre ORDER BY is_verified_country_avg_debut_year DESC), 3) AS is_verified_country_avg_debut_year_percent_rank
    FROM without_matches
),
summarized_rows AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM tracks t WHERE t.artist_id = s.id) AS track_count,
        (SELECT COUNT(CASE WHEN t.is_explicit = TRUE THEN 1 END) FROM tracks t WHERE t.artist_id = s.id) AS track_is_explicit_count
    FROM sequenced s
),
screened AS (
    -- Rows with no matching tracks
    SELECT s.*
    FROM summarized_rows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.id
              AND t.release_date IS NOT NULL
        )
      AND s.track_count BETWEEN 0 AND 6
)
-- Final projection
SELECT
    id,
    artist_name,
    genre,
    country,
    debut_year,
    genre_clean,
    genre_country_label,
    debut_year_avg_1_prior,
    is_verified_country_avg_debut_year,
    avg_debut_year,
    debut_year_minus_avg,
    is_verified_country_avg_debut_year_tercile,
    max_debut_year_minus_avg_so_far,
    is_verified_country_avg_debut_year_percent_rank,
    track_is_explicit_count,
    track_is_explicit_count + debut_year_avg_1_prior AS track_is_explicit_count_plus_debut_year_avg_1_pr
FROM screened
WHERE debut_year_minus_avg < 404.6
ORDER BY debut_year_minus_avg NULLS LAST, is_verified_country_avg_debut_year
