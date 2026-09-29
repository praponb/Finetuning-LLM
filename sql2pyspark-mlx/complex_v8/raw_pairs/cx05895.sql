WITH base_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        is_skipped,
        source
    FROM plays
)
-- Final projection
SELECT
    id,
    track_id,
    is_skipped,
    UPPER(TRIM(source)) AS source_clean
FROM base_plays
ORDER BY source_clean DESC, id NULLS LAST
