WITH covered AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.listener_id,
        a.played_at,
        COUNT(b.id) AS prior_rows_7d,
        SUM(b.ms_played) AS prior_ms_played_7d
    FROM plays a
    LEFT JOIN plays b
      ON b.listener_id = a.listener_id
     AND b.played_at < a.played_at
     AND b.played_at >= a.played_at - INTERVAL 7 DAYS
    GROUP BY a.id, a.listener_id, a.played_at
),
with_children_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.played_at,
        p.id AS play_id,
        p.source AS play_source,
        p.device AS play_device,
        p.ms_played AS play_ms_played,
        p.is_skipped AS play_is_skipped
    FROM covered s
    JOIN plays p ON p.listener_id = s.listener_id AND p.ms_played > 92277
)
-- Final projection
SELECT
    id,
    listener_id,
    played_at,
    play_id,
    play_is_skipped,
    LOWER(REGEXP_REPLACE(TRIM(play_source), ' ', '_')) AS play_source_slug,
    CONCAT_WS('-', play_device, play_source) AS play_device_play_source_label
FROM with_children_plays
ORDER BY play_device_play_source_label DESC
