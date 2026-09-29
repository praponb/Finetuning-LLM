WITH valid_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        source,
        ABS(ms_played - 241585) AS ms_played_distance
    FROM plays
    WHERE source IN ('playlist', 'search')
      AND track_id IS NOT NULL
),
with_stats AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM plays p WHERE p.track_id = s.track_id) AS play_count,
        (SELECT COUNT(*) FROM plays p WHERE p.track_id = s.track_id) AS play_count_2,
        (SELECT COUNT(CASE WHEN p.device = 'phone' THEN 1 END) FROM plays p WHERE p.track_id = s.track_id) AS play_device_count
    FROM valid_plays s
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT(source, '#', CAST(listener_id AS STRING)) AS source_ref,
        CONCAT(source, '#', CAST(id AS STRING)) AS source_ref_2
    FROM with_stats
    WHERE played_at >= TIMESTAMP '2025-01-31 12:00:00'
)
SELECT
    listener_id,
    play_device_count,
    GROUPING_ID() AS gid,
    COUNT(*) AS n_rows,
    MAX(play_count_2) AS max_play_count_2
FROM enriched
GROUP BY GROUPING SETS ((listener_id, play_device_count), (listener_id))
ORDER BY listener_id NULLS LAST, play_device_count NULLS LAST
