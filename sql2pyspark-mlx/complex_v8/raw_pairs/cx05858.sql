WITH prepared_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        CONCAT(source, '#', CAST(ms_played AS STRING)) AS source_ref
    FROM plays
    WHERE track_id IS NOT NULL
      AND source = 'playlist'
),
play_stats AS (
    -- Per-listener plays metrics
    SELECT listener_id, COUNT(*) AS play_count
    FROM plays
    GROUP BY listener_id
),
profiled AS (
    -- Attach plays metrics
    SELECT
        s.*,
        COALESCE(p.play_count, 0) AS play_count
    FROM prepared_plays s
    LEFT JOIN play_stats p ON p.listener_id = s.listener_id
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN device IN ('desktop', 'speaker') THEN 'group_a' WHEN device = 'car' THEN 'group_b' ELSE 'other' END AS device_group,
        LEAST(ms_played, play_count) AS least_ms_played_play_count
    FROM profiled
    WHERE played_at < TIMESTAMP '2025-03-02 10:00:00'
)
SELECT
    device,
    track_id,
    device_group,
    SUM(CASE WHEN is_skipped = TRUE THEN 1 ELSE 0 END) AS n_is_skipped_true
FROM tagged
GROUP BY CUBE(device, track_id, device_group)
ORDER BY device NULLS LAST, track_id NULLS LAST, device_group NULLS LAST
