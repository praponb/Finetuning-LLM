WITH in_window AS (
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
exploded_detail_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        p.id AS play_id,
        p.device AS play_device,
        p.is_skipped AS play_is_skipped,
        p.played_at AS play_played_at,
        p.source AS play_source
    FROM in_window s
    LEFT JOIN plays p ON p.listener_id = s.listener_id AND p.device <> 'car'
),
with_parent AS (
    -- Join plays
    SELECT s.*, p.device AS play_device_2
    FROM exploded_detail_plays s
    JOIN plays p ON p.id = s.play_id
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        LENGTH(TRIM(play_device_2)) AS play_device_2_len,
        COALESCE(play_device, 'UNKNOWN') AS play_device_or_unknown
    FROM with_parent
    WHERE play_source <> 'album'
)
SELECT
    id,
    play_id,
    play_device,
    play_is_skipped,
    play_played_at,
    play_source,
    play_device_2,
    play_device_2_len
FROM tagged
ORDER BY play_device DESC
