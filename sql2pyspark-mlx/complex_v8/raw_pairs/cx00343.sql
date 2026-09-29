WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        CONCAT(device, '#', CAST(track_id AS STRING)) AS device_ref,
        played_at + INTERVAL 7 DAYS AS played_at_plus_7d
    FROM plays
    WHERE ms_played <= 318978
),
detail_rows_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.is_skipped,
        s.played_at_plus_7d,
        p.id AS play_id,
        p.track_id AS play_track_id,
        p.device AS play_device,
        p.is_skipped AS play_is_skipped,
        p.ms_played AS play_ms_played
    FROM cleaned s
    LEFT JOIN plays p ON p.listener_id = s.listener_id AND NOT p.is_skipped
),
play_device_stats AS (
    -- Avg play_ms_played per play_device
    SELECT play_device, AVG(play_ms_played) AS avg_play_ms_played
    FROM detail_rows_plays
    GROUP BY play_device
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_play_ms_played,
        ROUND(s.play_ms_played - g.avg_play_ms_played, 2) AS play_ms_played_minus_avg
    FROM detail_rows_plays s
    JOIN play_device_stats g ON g.play_device = s.play_device
    WHERE s.play_ms_played > g.avg_play_ms_played
)
-- Subtotals with GROUPING SETS
SELECT
    play_track_id,
    play_is_skipped,
    ROUND(100 * COUNT(CASE WHEN play_device IS NOT NULL THEN 1 END) / COUNT(*), 1) AS pct_play_device_is_not_null,
    PERCENTILE_APPROX(play_ms_played, 0.5) AS median_play_ms_played,
    ROUND(SUM(play_ms_played_minus_avg) / NULLIF(SUM(play_ms_played), 0), 3) AS play_ms_played_minus_avg_to_play_ms_played_ratio
FROM vs_group
GROUP BY GROUPING SETS ((play_track_id, play_is_skipped), (play_track_id), ())
