WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_name,
        genre,
        country,
        is_verified,
        CASE WHEN debut_year >= 2018 THEN 'HIGH' WHEN debut_year >= 1996 THEN 'MEDIUM' ELSE 'LOW' END AS debut_year_tier,
        SUBSTRING(TRIM(country), 1, 4) AS country_prefix,
        CASE WHEN debut_year >= 2006 THEN 'A' WHEN debut_year >= 1997 THEN 'B' ELSE 'C' END AS debut_year_tier_2
    FROM artists
    WHERE country <> 'TH'
),
summarized_rows AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        (SELECT MIN(t.release_date) FROM tracks t WHERE t.artist_id = s.id) AS first_track_release_date,
        (SELECT COUNT(*) FROM tracks t WHERE t.artist_id = s.id) AS track_count,
        (SELECT COUNT(*) FROM tracks t WHERE t.artist_id = s.id) AS track_count_2
    FROM recent s
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN is_verified THEN 1 ELSE 0 END AS is_verified_flag,
        COALESCE(track_count, 0) AS track_count_filled
    FROM summarized_rows
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        YEAR(first_track_release_date) AS first_track_release_date_year,
        SUBSTRING(TRIM(country), 1, 4) AS country_prefix_2,
        ABS(track_count_filled - 5) AS track_count_filled_distance
    FROM enriched
)
-- Subtotals with ROLLUP
SELECT
    track_count_filled,
    track_count_2,
    GROUPING_ID() AS gid,
    SUM(track_count_filled_distance) AS total_track_count_filled_distance,
    COUNT(*) AS cnt
FROM scored
GROUP BY ROLLUP(track_count_filled, track_count_2)
ORDER BY track_count_filled NULLS LAST, track_count_2 NULLS LAST
