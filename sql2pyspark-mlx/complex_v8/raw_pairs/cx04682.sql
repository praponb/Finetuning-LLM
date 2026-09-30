WITH scoped_plays AS (
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
        COALESCE(ms_played, 0) AS ms_played_filled,
        DATE_FORMAT(played_at, 'MMM yyyy') AS played_at_label,
        DATE_FORMAT(played_at, 'yyyy-MM-dd HH:00') AS played_at_label_2
    FROM plays
    WHERE listener_id >= 7
      AND played_at < TIMESTAMP '2025-02-12 11:00:00'
),
play_stats AS (
    -- Per-listener plays metrics
    SELECT
        listener_id,
        COUNT(*) AS play_count,
        COUNT(*) FILTER (WHERE is_skipped = TRUE) AS play_is_skipped_count,
        COUNT(*) AS play_count_2
    FROM plays
    GROUP BY listener_id
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        COALESCE(p.play_count, 0) AS play_count,
        COALESCE(p.play_is_skipped_count, 0) AS play_is_skipped_count,
        COALESCE(p.play_count_2, 0) AS play_count_2
    FROM scoped_plays s
    LEFT JOIN play_stats p ON p.listener_id = s.listener_id
),
tagged AS (
    -- Derive helper columns
    SELECT *, DATE_FORMAT(played_at, 'yyyy-MM-dd HH:00') AS played_at_label_3
    FROM summarized_rows
),
tagged_2 AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN play_is_skipped_count >= 5 THEN 'large' WHEN play_is_skipped_count >= 2 THEN 'medium' ELSE 'small' END AS play_is_skipped_count_tier,
        COALESCE(device, 'UNKNOWN') AS device_or_unknown
    FROM tagged
)
-- Subtotals with GROUPING SETS
SELECT
    play_is_skipped_count,
    play_count_2,
    device_or_unknown,
    SUM(play_is_skipped_count) AS total_play_is_skipped_count,
    ROUND(SUM(play_is_skipped_count) / NULLIF(SUM(ms_played), 0), 3) AS play_is_skipped_count_to_ms_played_ratio
FROM tagged_2
GROUP BY GROUPING SETS ((play_is_skipped_count, play_count_2, device_or_unknown), (play_is_skipped_count), (play_count_2), ())
