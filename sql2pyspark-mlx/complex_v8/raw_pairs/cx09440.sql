WITH scoped AS (
    -- Filter plays
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
    WHERE HOUR(played_at) BETWEEN 9 AND 15
),
unique_rows AS (
    -- Keep the earliest row per source
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY source ORDER BY played_at, id) AS row_num
        FROM scoped
    ) ranked
    WHERE row_num = 1
),
with_stats AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT MIN(p.played_at) FROM plays p WHERE p.listener_id = s.listener_id) AS first_played_at,
        (SELECT COUNT(*) FROM plays p WHERE p.listener_id = s.listener_id) AS play_count
    FROM unique_rows s
)
-- Final projection
SELECT
    id,
    listener_id,
    track_id,
    played_at,
    ms_played,
    device,
    is_skipped,
    source,
    first_played_at,
    play_count,
    CASE WHEN play_count >= 5 THEN 'HIGH' WHEN play_count >= 3 THEN 'MEDIUM' ELSE 'LOW' END AS play_count_tier
FROM with_stats
ORDER BY play_count DESC
