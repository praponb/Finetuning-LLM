WITH base_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source
    FROM plays
    WHERE source IN ('search', 'album')
      AND device = 'car'
),
with_children_plays AS (
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
        p.id AS play_id,
        p.source AS play_source,
        p.is_skipped AS play_is_skipped
    FROM base_plays s
    JOIN plays p ON p.listener_id = s.listener_id
)
SELECT
    play_is_skipped,
    play_id,
    ROUND(STDDEV(ms_played), 2) AS stddev_ms_played,
    SUM(ms_played) AS total_ms_played,
    MAX(played_at) AS last_played_at
FROM with_children_plays
GROUP BY GROUPING SETS ((play_is_skipped, play_id), (play_is_skipped), (play_id), ())
