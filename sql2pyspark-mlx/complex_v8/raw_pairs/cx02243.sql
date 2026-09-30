WITH prepared_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        ABS(ms_played - 112786) AS ms_played_distance,
        MONTH(played_at) AS played_at_month,
        CASE WHEN device IN ('tv', 'phone') THEN 'group_a' WHEN device = 'desktop' THEN 'group_b' ELSE 'other' END AS device_group
    FROM plays
    WHERE device = 'car'
      AND listener_id <= 11
),
detail_rows_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.device,
        s.is_skipped,
        s.device_group,
        p.id AS play_id,
        p.source AS play_source,
        p.device AS play_device,
        p.is_skipped AS play_is_skipped,
        p.ms_played AS play_ms_played
    FROM prepared_plays s
    JOIN plays p ON p.listener_id = s.listener_id AND p.device IN ('phone', 'speaker', 'desktop')
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        play_ms_played - LAG(play_ms_played) OVER (PARTITION BY device, is_skipped ORDER BY ms_played, id, play_id) AS play_ms_played_change,
        DENSE_RANK() OVER (PARTITION BY device, is_skipped ORDER BY ms_played) AS ms_played_dense_rank,
        MAX(ms_played) OVER (PARTITION BY play_id, play_device) AS play_id_play_device_max_ms_played
    FROM detail_rows_plays
)
SELECT
    play_device,
    is_skipped,
    device,
    GROUPING(play_device) AS is_total_play_device,
    COUNT(*) AS cnt,
    ROUND(AVG(play_ms_played_change), 2) AS avg_play_ms_played_change
FROM windowed
GROUP BY CUBE(play_device, is_skipped, device)
