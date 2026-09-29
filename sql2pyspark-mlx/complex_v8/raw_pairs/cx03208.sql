WITH filtered AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        device,
        is_skipped,
        source
    FROM plays
    WHERE source = 'radio'
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        LOWER(TRIM(source)) AS source_clean,
        COALESCE(device, 'UNKNOWN') AS device_or_unknown
    FROM filtered
)
SELECT
    id,
    listener_id,
    device,
    is_skipped,
    source,
    source_clean,
    LENGTH(TRIM(device_or_unknown)) AS device_or_unknown_len,
    LENGTH(TRIM(source_clean)) AS source_clean_len
FROM tagged
WHERE listener_id IS NOT NULL
ORDER BY device DESC NULLS FIRST, source_clean, id
LIMIT 5
