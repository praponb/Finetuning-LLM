WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source
    FROM plays
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket,
        LENGTH(TRIM(device)) AS device_len,
        LENGTH(TRIM(source)) AS source_len
    FROM base
),
deduped AS (
    -- Keep the latest row per source
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        ms_played_bucket,
        device_len,
        source_len
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY source ORDER BY ms_played_bucket DESC, id DESC) AS row_num
        FROM annotated
    ) ranked
    WHERE row_num = 1
)
SELECT
    id,
    listener_id,
    track_id,
    device,
    is_skipped,
    source,
    ms_played_bucket,
    device_len,
    source_len,
    UPPER(TRIM(device)) AS device_clean
FROM deduped
ORDER BY source_len DESC, id
LIMIT 20
