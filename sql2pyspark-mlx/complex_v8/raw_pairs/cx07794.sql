WITH filtered_artists AS (
    -- Filter artists
    SELECT
        id,
        artist_name,
        genre,
        country,
        is_verified,
        CASE WHEN is_verified THEN 1 ELSE 0 END AS is_verified_flag
    FROM artists
    WHERE country <> 'JP'
      AND genre NOT IN ('Pop', 'Indie')
),
country_stats AS (
    -- Median is_verified_flag per country
    SELECT country, PERCENTILE_APPROX(is_verified_flag, 0.5) AS median_is_verified_flag
    FROM filtered_artists
    GROUP BY country
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_is_verified_flag,
        ROUND(s.is_verified_flag - g.median_is_verified_flag, 2) AS is_verified_flag_minus_median
    FROM filtered_artists s
    JOIN country_stats g ON g.country = s.country
    WHERE s.is_verified_flag >= g.median_is_verified_flag
),
profiled AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        (SELECT MIN(t.bpm) FROM tracks t WHERE t.artist_id = s.id) AS min_track_bpm,
        (SELECT MIN(t.release_date) FROM tracks t WHERE t.artist_id = s.id) AS first_track_release_date,
        (SELECT SUM(t.duration_sec) FROM tracks t WHERE t.artist_id = s.id) AS total_track_duration_sec
    FROM vs_group s
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        artist_name,
        country,
        is_verified,
        is_verified_flag_minus_median,
        min_track_bpm,
        total_track_duration_sec,
        SUM(min_track_bpm) OVER w_is_verified_genre AS is_verified_genre_sum_min_track_bpm,
        MAX(is_verified_flag) OVER (ORDER BY total_track_duration_sec, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS is_verified_flag_max_6_prior,
        COUNT(*) OVER (PARTITION BY is_verified, genre) AS rows_in_is_verified_genre,
        ROUND(PERCENT_RANK() OVER w_all_by_total_track_duration_2, 3) AS total_track_duration_sec_percent_rank
    FROM profiled
    WINDOW w_is_verified_genre AS (PARTITION BY is_verified, genre), w_all_by_total_track_duration_2 AS (ORDER BY total_track_duration_sec)
)
-- Final projection
SELECT
    id,
    artist_name,
    country,
    is_verified,
    min_track_bpm,
    total_track_duration_sec,
    rows_in_is_verified_genre,
    total_track_duration_sec_percent_rank
FROM with_windows
ORDER BY rows_in_is_verified_genre DESC, artist_name NULLS LAST
