WITH filtered AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        COALESCE(ms_played, 0) AS ms_played_filled
    FROM plays
    WHERE (ms_played BETWEEN 108979 AND 241585 OR is_skipped = TRUE)
),
play_stats AS (
    -- Per-track plays metrics
    SELECT
        track_id,
        COUNT(*) AS play_count,
        COUNT(*) AS play_count_2,
        COUNT(*) AS play_count_3
    FROM plays
    GROUP BY track_id
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        COALESCE(p.play_count, 0) AS play_count,
        COALESCE(p.play_count_2, 0) AS play_count_2,
        COALESCE(p.play_count_3, 0) AS play_count_3
    FROM filtered s
    LEFT JOIN play_stats p ON p.track_id = s.track_id
),
with_parent_track AS (
    -- Join tracks
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.device,
        s.source,
        s.play_count,
        s.play_count_2,
        s.play_count_3,
        t.bpm AS track_bpm
    FROM summarized_rows s
    LEFT JOIN tracks t ON t.id = s.track_id
),
latest_per_key AS (
    -- Keep the latest row per track_id, device
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        source,
        play_count,
        play_count_2,
        play_count_3,
        track_bpm
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY track_id, device ORDER BY track_bpm DESC, id DESC) AS rn
        FROM with_parent_track
    ) ranked
    WHERE rn = 1
)
-- Final projection
SELECT
    id,
    listener_id,
    track_id,
    played_at,
    device,
    source,
    play_count,
    play_count_2,
    play_count_3,
    track_bpm,
    CASE WHEN source IN ('album', 'search') THEN 'group_a' WHEN source = 'radio' THEN 'group_b' ELSE 'other' END AS source_group
FROM latest_per_key
ORDER BY listener_id, device, id
LIMIT 5
