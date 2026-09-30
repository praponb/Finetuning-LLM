WITH scoped_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        source,
        CASE WHEN is_skipped THEN 1 ELSE 0 END AS is_skipped_flag,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket
    FROM plays
    WHERE HOUR(played_at) BETWEEN 8 AND 13
),
deduped AS (
    -- Keep the latest row per listener_id, source
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        source,
        is_skipped_flag,
        ms_played_bucket
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY listener_id, source ORDER BY is_skipped_flag DESC, id DESC) AS rn
        FROM scoped_plays
    ) ranked
    WHERE rn = 1
),
with_children_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.source,
        s.ms_played_bucket,
        p.id AS play_id,
        p.played_at AS play_played_at,
        p.device AS play_device
    FROM deduped s
    LEFT JOIN plays p ON p.listener_id = s.listener_id
)
-- Subtotals with CUBE
SELECT
    COALESCE(play_device, 'ALL') AS play_device,
    COALESCE(source, 'ALL') AS source,
    MIN(ms_played_bucket) AS min_ms_played_bucket,
    COUNT(*) AS row_count
FROM with_children_plays
GROUP BY CUBE(play_device, source)
ORDER BY play_device NULLS LAST, source NULLS LAST
