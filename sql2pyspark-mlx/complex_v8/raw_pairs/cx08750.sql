WITH base AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        release_date,
        bpm,
        moods,
        LENGTH(TRIM(title)) AS title_len,
        ABS(duration_sec - 183) AS duration_sec_distance
    FROM tracks
    WHERE bpm BETWEEN 75 AND 166
      AND is_explicit = FALSE
),
combined_details_artist AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.release_date,
        s.bpm,
        s.moods,
        s.title_len,
        s.duration_sec_distance,
        a.country AS artist_country,
        a.genre AS artist_genre
    FROM base s
    JOIN artists a ON a.id = s.artist_id
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (PARTITION BY artist_country, DATE_TRUNC('MONTH', release_date) ORDER BY bpm) AS bpm_rank,
        LAST_VALUE(title_len) IGNORE NULLS OVER (ORDER BY bpm DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_title_len,
        title_len - LAG(title_len) OVER (PARTITION BY artist_country, DATE_TRUNC('MONTH', release_date) ORDER BY bpm, id) AS title_len_change,
        ROUND(AVG(duration_sec_distance) OVER (PARTITION BY artist_country, DATE_TRUNC('MONTH', release_date) ORDER BY bpm, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS duration_sec_distance_avg_6_prior,
        MIN(title_len) OVER (ORDER BY bpm DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_title_len_so_far,
        ROUND(AVG(duration_sec_distance) OVER (PARTITION BY artist_country), 2) AS artist_country_avg_duration_sec_distance,
        ROUND(title_len - AVG(title_len) OVER (PARTITION BY artist_country), 2) AS title_len_vs_artist_country_avg,
        FIRST_VALUE(title_len) OVER (ORDER BY bpm DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_title_len,
        SIZE(COLLECT_SET(artist_id) OVER (PARTITION BY artist_country)) AS distinct_artist_id_in_artist_country,
        bpm - LAG(bpm) OVER (PARTITION BY artist_genre, MONTH(release_date) ORDER BY duration_sec_distance NULLS LAST, id) AS bpm_change,
        COUNT(*) OVER w_artist_country AS rows_in_artist_country,
        ROUND(PERCENT_RANK() OVER (ORDER BY bpm DESC NULLS FIRST), 3) AS bpm_percent_rank,
        RANK() OVER (ORDER BY bpm DESC NULLS FIRST) AS bpm_rank_2,
        duration_sec_distance - LAG(duration_sec_distance) OVER w_artist_genre_release_date_mo AS duration_sec_distance_change
    FROM combined_details_artist
    WINDOW w_artist_country AS (PARTITION BY artist_country), w_artist_genre_release_date_mo AS (PARTITION BY artist_genre, MONTH(release_date) ORDER BY duration_sec_distance NULLS LAST, id)
),
leaders AS (
    -- First row per group
    SELECT *
    FROM windowed
    WHERE bpm_rank = 1
)
-- Final projection
SELECT DISTINCT bpm_change, min_title_len_so_far, duration_sec_distance
FROM leaders
ORDER BY bpm_change NULLS LAST, min_title_len_so_far DESC
