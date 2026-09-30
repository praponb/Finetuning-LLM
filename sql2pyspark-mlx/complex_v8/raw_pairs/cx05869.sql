WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        bpm,
        moods,
        DATE_ADD(release_date, 30) AS release_date_plus_30d,
        TRUNC(release_date, 'MM') AS release_date_month_start,
        IF(is_explicit, 'yes', 'no') AS is_explicit_label
    FROM tracks
    WHERE bpm BETWEEN 76 AND 156
),
artist_id_stats AS (
    -- Avg bpm per artist_id
    SELECT artist_id, AVG(bpm) AS avg_bpm
    FROM cleaned
    GROUP BY artist_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_bpm, ROUND(s.bpm - g.avg_bpm, 2) AS bpm_minus_avg
    FROM cleaned s
    JOIN artist_id_stats g ON g.artist_id = s.artist_id
    WHERE s.bpm > g.avg_bpm
),
detail_rows AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.bpm,
        s.release_date_plus_30d,
        p.id AS play_id,
        p.is_skipped AS play_is_skipped,
        p.ms_played AS play_ms_played
    FROM above_average s
    LEFT JOIN plays p ON p.track_id = s.id
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        artist_id,
        MAX(release_date_plus_30d) AS last_release_date_plus_30d,
        PERCENTILE_APPROX(play_ms_played, 0.5) AS median_play_ms_played,
        SUM(duration_sec) FILTER (WHERE play_is_skipped = TRUE) AS duration_sec_play_is_skipped_true
    FROM detail_rows
    GROUP BY artist_id
),
track_stats AS (
    -- Per-artist tracks metrics
    SELECT
        artist_id,
        COUNT(*) AS track_count,
        MAX(release_date) AS last_track_release_date
    FROM tracks
    GROUP BY artist_id
),
summarized_rows AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        COALESCE(t.track_count, 0) AS track_count,
        t.last_track_release_date
    FROM grouped s
    LEFT JOIN track_stats t ON t.artist_id = s.artist_id
)
-- Final projection
SELECT
    artist_id,
    last_release_date_plus_30d,
    median_play_ms_played,
    duration_sec_play_is_skipped_true,
    track_count,
    last_track_release_date,
    track_count - median_play_ms_played AS track_count_minus_median_play_ms_played,
    GREATEST(median_play_ms_played, track_count) AS greatest_median_play_ms_played_track_count
FROM summarized_rows
WHERE median_play_ms_played <= 261750
ORDER BY greatest_median_play_ms_played_track_count DESC, last_track_release_date, artist_id
LIMIT 5
