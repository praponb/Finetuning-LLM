WITH base_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        LENGTH(TRIM(source)) AS source_len
    FROM plays
    WHERE is_skipped = TRUE
)
SELECT id, measure, reading
FROM (
    SELECT id, source_len, ms_played
    FROM base_plays
) src
UNPIVOT (
    reading FOR measure IN (source_len, ms_played)
) unp
ORDER BY id, measure
