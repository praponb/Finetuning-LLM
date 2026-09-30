WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        country,
        debut_year,
        is_verified,
        CONCAT(genre, '#', CAST(debut_year AS STRING)) AS genre_ref,
        ABS(debut_year - 2006) AS debut_year_distance,
        COALESCE(debut_year, 0) AS debut_year_filled
    FROM artists
    WHERE NOT is_verified
      AND country = 'US'
),
qualified AS (
    -- Compare against the overall max
    SELECT *
    FROM filtered
    WHERE debut_year_distance < (SELECT MAX(debut_year_distance) FROM filtered)
),
with_stats AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        (SELECT SUM(t.duration_sec) FROM tracks t WHERE t.artist_id = s.id) AS total_track_duration_sec,
        (SELECT MIN(t.bpm) FROM tracks t WHERE t.artist_id = s.id) AS min_track_bpm
    FROM qualified s
),
latest AS (
    -- Keep the earliest row per country
    SELECT
        id,
        country,
        debut_year,
        is_verified,
        genre_ref,
        debut_year_distance,
        debut_year_filled,
        total_track_duration_sec,
        min_track_bpm
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY country ORDER BY min_track_bpm, id) AS row_num
        FROM with_stats
    ) ranked
    WHERE row_num = 1
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(country, 'ALL') AS country,
    is_verified,
    ARRAY_SORT(COLLECT_SET(country)) AS country_list
FROM latest
GROUP BY GROUPING SETS ((country, is_verified), (country), (is_verified))
ORDER BY country NULLS LAST, is_verified NULLS LAST
