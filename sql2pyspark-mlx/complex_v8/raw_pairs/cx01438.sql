WITH cleaned AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm,
        moods,
        CASE WHEN is_explicit THEN 1 ELSE 0 END AS is_explicit_flag,
        YEAR(release_date) AS release_date_year,
        LENGTH(TRIM(title)) AS title_len
    FROM tracks
    WHERE UPPER(TRIM(title)) LIKE 'P%'
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER (ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER w_title_len, 0), 4) AS duration_sec_share_of_title_len,
        ROUND(title_len - AVG(title_len) OVER (PARTITION BY title_len), 2) AS title_len_vs_title_len_avg,
        NTILE(3) OVER (ORDER BY duration_sec, id) AS duration_sec_tercile,
        ROUND(AVG(duration_sec) OVER (PARTITION BY release_date_year, title_len ORDER BY bpm ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS duration_sec_avg_7,
        LEAD(title_len, 1, 0) OVER (PARTITION BY release_date_year, title_len ORDER BY bpm ASC, id) AS next_title_len
    FROM cleaned
    WINDOW w_title_len AS (PARTITION BY title_len)
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(CUME_DIST() OVER w_next_title_len_by_duration_s, 3) AS duration_sec_tercile_cume_dist,
        NTILE(4) OVER (PARTITION BY next_title_len ORDER BY duration_sec_tercile ASC, id) AS duration_sec_tercile_quartile,
        NTILE(10) OVER w_next_title_len_by_duration_s_2 AS duration_sec_tercile_decile,
        LAG(duration_sec) OVER (PARTITION BY next_title_len ORDER BY duration_sec_tercile ASC, id) AS prev_duration_sec,
        ROUND(next_title_len / NULLIF(SUM(next_title_len) OVER (PARTITION BY release_date_year), 0), 4) AS next_title_len_share_of_release_date_year
    FROM with_windows
    WINDOW w_next_title_len_by_duration_s AS (PARTITION BY next_title_len ORDER BY duration_sec_tercile ASC), w_next_title_len_by_duration_s_2 AS (PARTITION BY next_title_len ORDER BY duration_sec_tercile ASC, id)
),
screened AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM windowed s
    WHERE s.id IN (
            SELECT p.track_id
            FROM plays p
            WHERE p.played_at < TIMESTAMP '2025-02-24 12:00:00'
        )
),
artist_id_stats AS (
    -- Median duration_sec_tercile_decile per artist_id
    SELECT artist_id, PERCENTILE_APPROX(duration_sec_tercile_decile, 0.5) AS median_duration_sec_tercile_decile
    FROM screened
    GROUP BY artist_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_duration_sec_tercile_decile,
        ROUND(s.duration_sec_tercile_decile - g.median_duration_sec_tercile_decile, 2) AS duration_sec_tercile_decile_minus_median
    FROM screened s
    JOIN artist_id_stats g ON g.artist_id = s.artist_id
    WHERE s.duration_sec_tercile_decile >= g.median_duration_sec_tercile_decile
),
track_stats AS (
    -- Per-artist tracks metrics
    SELECT
        artist_id,
        MIN(release_date) AS first_track_release_date,
        COUNT(*) AS track_count,
        COUNT(CASE WHEN NOT is_explicit THEN 1 END) AS track_is_explicit_count
    FROM tracks
    GROUP BY artist_id
),
profiled AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        t.first_track_release_date,
        COALESCE(t.track_count, 0) AS track_count,
        COALESCE(t.track_is_explicit_count, 0) AS track_is_explicit_count
    FROM outliers s
    LEFT JOIN track_stats t ON t.artist_id = s.artist_id
),
with_parent AS (
    -- Join artists
    SELECT s.*, a.genre AS artist_genre
    FROM profiled s
    JOIN artists a ON a.id = s.artist_id
    WHERE a.genre = 'Pop'
)
-- Final projection
SELECT
    id,
    artist_id,
    title,
    duration_sec,
    release_date,
    moods,
    is_explicit_flag,
    title_len,
    running_count,
    title_len_vs_title_len_avg,
    duration_sec_tercile,
    duration_sec_avg_7,
    next_title_len,
    duration_sec_tercile_cume_dist,
    duration_sec_tercile_quartile,
    duration_sec_tercile_decile,
    next_title_len_share_of_release_date_year,
    median_duration_sec_tercile_decile,
    duration_sec_tercile_decile_minus_median,
    track_is_explicit_count,
    SUBSTRING(TRIM(artist_genre), 1, 3) AS artist_genre_prefix
FROM with_parent
WHERE duration_sec_tercile_quartile <= 9
ORDER BY next_title_len_share_of_release_date_year, track_is_explicit_count
