WITH base AS (
    -- Filter artists
    SELECT
        id,
        artist_name,
        genre,
        country,
        debut_year,
        is_verified,
        FLOOR(debut_year / 5) * 5 AS debut_year_bucket
    FROM artists
    WHERE NOT is_verified
),
track_stats AS (
    -- Per-artist tracks metrics
    SELECT
        artist_id,
        MIN(release_date) AS first_track_release_date,
        COUNT(*) AS track_count
    FROM tracks
    GROUP BY artist_id
),
with_counts AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        t.first_track_release_date,
        COALESCE(t.track_count, 0) AS track_count
    FROM base s
    LEFT JOIN track_stats t ON t.artist_id = s.id
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        genre,
        country,
        YEAR(first_track_release_date) AS first_track_release_date_year,
        SUM(CASE WHEN country = 'JP' THEN 1 ELSE 0 END) AS n_country_jp,
        ARRAY_SORT(COLLECT_SET(country)) AS country_list
    FROM with_counts
    WHERE track_count BETWEEN 0 AND 8
    GROUP BY genre, country, YEAR(first_track_release_date)
    HAVING SUM(debut_year) > 1996
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM rollup_base
    WHERE first_track_release_date_year > (SELECT AVG(first_track_release_date_year) FROM rollup_base)
)
SELECT
    genre,
    country,
    first_track_release_date_year,
    country_list,
    COALESCE(n_country_jp, 0) AS n_country_jp_filled
FROM screened
ORDER BY genre, country
