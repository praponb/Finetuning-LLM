WITH filtered_artists AS (
    -- Scope the artists rows
    SELECT
        id,
        artist_name,
        country,
        debut_year,
        is_verified,
        LENGTH(TRIM(artist_name)) AS artist_name_len
    FROM artists
    WHERE NOT is_verified
),
profiled AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        (SELECT MAX(t.release_date) FROM tracks t WHERE t.artist_id = s.id) AS last_track_release_date,
        (SELECT COUNT(*) FROM tracks t WHERE t.artist_id = s.id) AS track_count,
        (SELECT MIN(t.duration_sec) FROM tracks t WHERE t.artist_id = s.id) AS min_track_duration_sec
    FROM filtered_artists s
),
country_stats AS (
    -- Median min_track_duration_sec per country
    SELECT country, PERCENTILE_APPROX(min_track_duration_sec, 0.5) AS median_min_track_duration_sec
    FROM profiled
    GROUP BY country
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_min_track_duration_sec,
        ROUND(s.min_track_duration_sec - g.median_min_track_duration_sec, 2) AS min_track_duration_sec_minus_median
    FROM profiled s
    JOIN country_stats g ON g.country = s.country
    WHERE s.min_track_duration_sec >= g.median_min_track_duration_sec
)
SELECT
    id,
    artist_name,
    country,
    is_verified,
    track_count,
    min_track_duration_sec,
    DATE_FORMAT(last_track_release_date, 'MMM yyyy') AS last_track_release_date_label,
    LOWER(REGEXP_REPLACE(TRIM(country), ' ', '_')) AS country_slug
FROM outliers
