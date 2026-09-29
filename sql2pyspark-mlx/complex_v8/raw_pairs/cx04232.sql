WITH scoped_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source
    FROM plays
    WHERE device = 'car'
      AND NOT is_skipped
),
exploded_detail_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.is_skipped,
        s.source,
        p.id AS play_id,
        p.source AS play_source,
        p.is_skipped AS play_is_skipped,
        p.played_at AS play_played_at,
        p.device AS play_device
    FROM scoped_plays s
    JOIN plays p ON p.track_id = s.track_id
)
SELECT
    listener_id,
    COALESCE(play_device, 'ALL') AS play_device,
    track_id,
    GROUPING(listener_id) AS is_total_listener_id,
    MAX(play_played_at) AS last_play_played_at,
    ARRAY_SORT(COLLECT_SET(source)) AS source_list,
    SUM(CASE WHEN is_skipped = TRUE THEN 1 ELSE 0 END) AS n_is_skipped_true
FROM exploded_detail_plays
GROUP BY ROLLUP(listener_id, play_device, track_id)
