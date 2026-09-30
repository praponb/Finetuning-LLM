WITH filtered_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        LOWER(TRIM(source)) AS source_clean,
        TO_DATE(played_at) AS played_at_date
    FROM plays
    WHERE is_skipped = TRUE
      AND track_id <= 17
),
detail_rows_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.device,
        s.is_skipped,
        s.source,
        s.source_clean,
        s.played_at_date,
        p.id AS play_id,
        p.is_skipped AS play_is_skipped,
        p.device AS play_device,
        p.played_at AS play_played_at,
        p.ms_played AS play_ms_played
    FROM filtered_plays s
    JOIN plays p ON p.track_id = s.track_id
)
-- Subtotals with GROUPING SETS
SELECT
    device,
    play_is_skipped,
    COUNT(*) AS n_rows,
    ROUND(100 * COUNT(CASE WHEN play_is_skipped = TRUE THEN 1 END) / COUNT(*), 1) AS pct_play_is_skipped_true,
    COUNT(DISTINCT source) AS distinct_sources
FROM detail_rows_plays
GROUP BY GROUPING SETS ((device, play_is_skipped), (device), (play_is_skipped), ())
