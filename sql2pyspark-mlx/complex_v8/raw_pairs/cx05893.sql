WITH matched AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.listener_id,
        a.played_at,
        COUNT(b.id) AS prior_rows_1d,
        SUM(b.ms_played) AS prior_ms_played_1d
    FROM plays a
    LEFT JOIN plays b
      ON b.listener_id = a.listener_id
     AND b.played_at < a.played_at
     AND b.played_at >= a.played_at - INTERVAL 1 DAYS
    GROUP BY a.id, a.listener_id, a.played_at
),
qualified AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM matched s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.listener_id
        )
),
detail_rows_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        p.id AS play_id,
        p.ms_played AS play_ms_played,
        p.source AS play_source,
        p.device AS play_device
    FROM qualified s
    JOIN plays p ON p.listener_id = s.listener_id
)
SELECT
    id,
    listener_id,
    play_id,
    play_source,
    play_device,
    CONCAT_WS('|', play_source, play_device) AS play_source_play_device_label
FROM detail_rows_plays
