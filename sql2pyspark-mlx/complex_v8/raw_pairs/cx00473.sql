WITH scoped_artists AS (
    -- Scope the artists rows
    SELECT *
    FROM artists
    WHERE debut_year < 2016
),
without_matches AS (
    -- Rows that have matching tracks
    SELECT s.*
    FROM scoped_artists s
    WHERE EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.id
              AND UPPER(TRIM(t.title)) LIKE 'S%'
        )
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        CONCAT_WS(': ', country, genre) AS country_genre_label,
        COALESCE(debut_year, 0) AS debut_year_filled
    FROM without_matches
    WHERE country IS NOT NULL
),
without_matches_2 AS (
    -- Rows that have matching tracks
    SELECT s.*
    FROM scored s
    WHERE s.id IN (
            SELECT t.artist_id
            FROM tracks t
        )
),
country_stats AS (
    -- Avg debut_year_filled per country
    SELECT country, AVG(debut_year_filled) AS avg_debut_year_filled
    FROM without_matches_2
    GROUP BY country
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_debut_year_filled,
        ROUND(s.debut_year_filled - g.avg_debut_year_filled, 2) AS debut_year_filled_minus_avg
    FROM without_matches_2 s
    JOIN country_stats g ON g.country = s.country
    WHERE s.debut_year_filled > g.avg_debut_year_filled
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(avg_debut_year_filled / NULLIF(SUM(avg_debut_year_filled) OVER (PARTITION BY genre, is_verified), 0), 4) AS avg_debut_year_filled_share_of_genre_is_verified,
        NTH_VALUE(debut_year_filled_minus_avg, 2) OVER (PARTITION BY country ORDER BY debut_year_filled_minus_avg DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_debut_year_filled_minus_avg
    FROM vs_group
),
stats AS (
    -- Group-level totals
    SELECT
        country,
        is_verified,
        genre,
        ROUND(SUM(avg_debut_year_filled) / NULLIF(SUM(debut_year_filled), 0), 3) AS avg_debut_year_filled_to_debut_year_filled_ratio,
        ROUND(SUM(avg_debut_year_filled_share_of_genre_is_verified) / NULLIF(SUM(debut_year_filled_minus_avg), 0), 3) AS avg_debut_year_filled_share_of_genre_is_verified_2,
        MAX(second_debut_year_filled_minus_avg) AS max_second_debut_year_filled_minus_avg,
        SUM(debut_year_filled) FILTER (WHERE genre <> 'Jazz') AS debut_year_filled_genre_jazz,
        SUM(CASE WHEN country <> 'TH' THEN avg_debut_year_filled ELSE 0 END) AS avg_debut_year_filled_country_th,
        COUNT(DISTINCT genre) AS distinct_genres
    FROM with_windows
    WHERE UPPER(TRIM(artist_name)) LIKE 'L%'
    GROUP BY country, is_verified, genre
    HAVING SUM(second_debut_year_filled_minus_avg) > -606.9
)
SELECT *
FROM (
    SELECT distinct_genres, genre, avg_debut_year_filled_to_debut_year_filled_ratio
    FROM stats
) src
PIVOT (
    COUNT(avg_debut_year_filled_to_debut_year_filled_ratio)
    FOR genre IN ('Pop', 'Classical', 'HipHop', 'Indie')
)
