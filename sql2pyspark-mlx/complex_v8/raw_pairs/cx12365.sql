WITH scoped_plays AS (
    -- Scope the plays rows
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
    WHERE track_id IS NOT NULL
),
play_stats AS (
    -- Per-track plays metrics
    SELECT track_id, COUNT(*) AS play_count
    FROM plays
    GROUP BY track_id
),
profiled AS (
    -- Attach plays metrics
    SELECT
        s.*,
        COALESCE(p.play_count, 0) AS play_count
    FROM scoped_plays s
    LEFT JOIN play_stats p ON p.track_id = s.track_id
),
latest_per_key AS (
    -- Keep the latest row per track_id
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        play_count
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY track_id ORDER BY ms_played DESC, id DESC) AS row_num
        FROM profiled
    ) ranked
    WHERE row_num = 1
)
-- Final projection
SELECT
    id,
    listener_id,
    played_at,
    ms_played,
    device,
    is_skipped,
    play_count,
    CASE WHEN device IN ('desktop', 'tv') THEN 'group_a' WHEN device = 'phone' THEN 'group_b' ELSE 'other' END AS device_group
FROM latest_per_key
ORDER BY device_group DESC
