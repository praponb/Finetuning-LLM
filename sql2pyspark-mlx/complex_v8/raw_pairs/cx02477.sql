WITH prepared_artists AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_name,
        genre,
        country,
        is_verified,
        COALESCE(genre, 'UNKNOWN') AS genre_or_unknown,
        UPPER(TRIM(country)) AS country_clean,
        CONCAT(genre, '#', CAST(debut_year AS STRING)) AS genre_ref
    FROM artists
    WHERE is_verified = TRUE
),
with_stats AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        (SELECT MAX(t.release_date) FROM tracks t WHERE t.artist_id = s.id) AS last_track_release_date,
        (SELECT COUNT(*) FROM tracks t WHERE t.artist_id = s.id) AS track_count
    FROM prepared_artists s
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        artist_name,
        genre,
        country,
        is_verified,
        genre_or_unknown,
        country_clean,
        genre_ref,
        NTILE(5) OVER (PARTITION BY genre_or_unknown, country ORDER BY track_count ASC, id) AS track_count_quintile,
        DATEDIFF(last_track_release_date, LAG(last_track_release_date) OVER w_last_track_release_date_year) AS days_since_prev_last_track_release_date,
        SIZE(COLLECT_SET(country) OVER (PARTITION BY is_verified)) AS distinct_country_in_is_verified,
        ROUND(CUME_DIST() OVER (PARTITION BY YEAR(last_track_release_date) ORDER BY track_count), 3) AS track_count_cume_dist,
        SIZE(COLLECT_SET(genre) OVER w_is_verified) AS distinct_genre_in_is_verified
    FROM with_stats
    WINDOW w_last_track_release_date_year AS (PARTITION BY YEAR(last_track_release_date) ORDER BY track_count, id), w_is_verified AS (PARTITION BY is_verified)
),
genre_stats AS (
    -- Avg days_since_prev_last_track_release_date per genre
    SELECT genre, AVG(days_since_prev_last_track_release_date) AS avg_days_since_prev_last_track_release_date
    FROM with_windows
    GROUP BY genre
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_days_since_prev_last_track_release_date,
        ROUND(s.days_since_prev_last_track_release_date - g.avg_days_since_prev_last_track_release_date, 2) AS days_since_prev_last_track_release_date_minus_av
    FROM with_windows s
    JOIN genre_stats g ON g.genre = s.genre
    WHERE s.days_since_prev_last_track_release_date > g.avg_days_since_prev_last_track_release_date
)
SELECT *
FROM (
    SELECT track_count_quintile, genre_or_unknown, distinct_genre_in_is_verified
    FROM outliers
) src
PIVOT (
    SUM(distinct_genre_in_is_verified)
    FOR genre_or_unknown IN ('Indie', 'Classical')
)
ORDER BY track_count_quintile
