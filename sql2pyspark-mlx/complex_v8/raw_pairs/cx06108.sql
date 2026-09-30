WITH scoped_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        TO_DATE(played_at) AS played_at_date
    FROM plays
),
with_activity AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM scoped_plays s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.track_id
              AND p.source <> 'search'
        )
),
play_stats AS (
    -- Per-listener plays metrics
    SELECT
        listener_id,
        COUNT(CASE WHEN is_skipped = TRUE THEN 1 END) AS play_is_skipped_count,
        MIN(played_at) AS first_played_at,
        SUM(ms_played) AS total_play_ms_played
    FROM plays
    GROUP BY listener_id
),
with_counts AS (
    -- Attach plays metrics
    SELECT
        s.*,
        COALESCE(p.play_is_skipped_count, 0) AS play_is_skipped_count,
        p.first_played_at,
        p.total_play_ms_played
    FROM with_activity s
    LEFT JOIN play_stats p ON p.listener_id = s.listener_id
),
detail_rows_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.device,
        s.played_at_date,
        s.play_is_skipped_count,
        s.first_played_at,
        p.id AS play_id,
        p.source AS play_source,
        p.device AS play_device,
        p.ms_played AS play_ms_played
    FROM with_counts s
    LEFT JOIN plays p ON p.track_id = s.track_id
)
-- Subtotals with CUBE
SELECT
    play_id,
    track_id,
    COUNT(DISTINCT play_source) AS distinct_play_sources,
    MIN(played_at_date) AS first_played_at_date
FROM detail_rows_plays
GROUP BY CUBE(play_id, track_id)
