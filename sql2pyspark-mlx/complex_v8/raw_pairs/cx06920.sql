WITH cleaned_artists AS (
    -- Filter artists
    SELECT
        id,
        artist_name,
        genre,
        country,
        debut_year,
        is_verified,
        FLOOR(debut_year / 5) * 5 AS debut_year_bucket,
        CASE WHEN debut_year >= 2016 THEN 'A' WHEN debut_year >= 1994 THEN 'B' ELSE 'C' END AS debut_year_tier,
        COALESCE(country, 'UNKNOWN') AS country_or_unknown
    FROM artists
    WHERE debut_year < 2015
),
with_stats AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        (SELECT MIN(t.release_date) FROM tracks t WHERE t.artist_id = s.id) AS first_track_release_date,
        (SELECT MIN(t.release_date) FROM tracks t WHERE t.artist_id = s.id) AS first_track_release_date_2,
        (SELECT COUNT(*) FROM tracks t WHERE t.artist_id = s.id) AS track_count
    FROM cleaned_artists s
)
-- Subtotals with ROLLUP
SELECT
    debut_year_tier,
    country_or_unknown,
    GROUPING_ID() AS gid,
    ARRAY_SORT(COLLECT_SET(genre)) AS genre_list
FROM with_stats
GROUP BY ROLLUP(debut_year_tier, country_or_unknown)
