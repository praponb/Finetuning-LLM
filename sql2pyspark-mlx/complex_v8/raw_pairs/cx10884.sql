WITH recent AS (
    -- Scope the artists rows
    SELECT
        id,
        genre,
        country,
        is_verified,
        ABS(debut_year - 2015) AS debut_year_distance
    FROM artists
    WHERE is_verified = TRUE
),
screened AS (
    -- Rows that have matching tracks
    SELECT s.*
    FROM recent s
    WHERE EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.id
        )
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        is_verified,
        debut_year_distance,
        CASE WHEN debut_year_distance >= 1214 THEN 'A' WHEN debut_year_distance >= 809 THEN 'B' ELSE 'C' END AS debut_year_distance_tier,
        COALESCE(country, 'UNKNOWN') AS country_or_unknown,
        CONCAT_WS(' / ', country, genre) AS country_genre_label
    FROM screened
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        debut_year_distance,
        country_or_unknown,
        country_genre_label,
        NTILE(10) OVER (ORDER BY debut_year_distance, id) AS debut_year_distance_decile,
        ROUND(debut_year_distance - AVG(debut_year_distance) OVER w_debut_year_distance_tier, 2) AS debut_year_distance_vs_debut_year_distance_tier_
    FROM enriched
    WINDOW w_debut_year_distance_tier AS (PARTITION BY debut_year_distance_tier)
)
-- Subtotals with GROUPING SETS
SELECT
    debut_year_distance_decile,
    country_or_unknown,
    SUM(debut_year_distance_decile) AS total_debut_year_distance_decile,
    COUNT(*) AS cnt,
    ROUND(SUM(debut_year_distance_vs_debut_year_distance_tier_) / NULLIF(SUM(debut_year_distance_decile), 0), 3) AS debut_year_distance_vs_debut_year_distance_tier__2
FROM sequenced
GROUP BY GROUPING SETS ((debut_year_distance_decile, country_or_unknown), (debut_year_distance_decile), (country_or_unknown), ())
ORDER BY debut_year_distance_decile NULLS LAST, country_or_unknown NULLS LAST
