WITH prepared AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        device,
        is_skipped,
        source,
        DATE_FORMAT(played_at, 'yyyy-MM-dd HH:00') AS played_at_label
    FROM plays
),
qualified AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM prepared s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.track_id
              AND p.listener_id >= 11
        )
      AND s.source IN ('playlist', 'radio')
),
enriched_track AS (
    -- Join tracks
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.device,
        s.is_skipped,
        s.source,
        t.duration_sec AS track_duration_sec
    FROM qualified s
    JOIN tracks t ON t.id = s.track_id
    WHERE t.duration_sec >= 165
)
-- Final projection
SELECT DISTINCT track_duration_sec, track_id
FROM enriched_track
