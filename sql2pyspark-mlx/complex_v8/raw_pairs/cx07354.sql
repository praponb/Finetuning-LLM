WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        CONCAT_WS('-', source, device) AS source_device_label,
        DATE_FORMAT(played_at, 'yyyy-MM-dd HH:00') AS played_at_label
    FROM plays
    WHERE track_id IS NOT NULL
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        UPPER(TRIM(played_at_label)) AS played_at_label_clean,
        TO_DATE(played_at) AS played_at_date
    FROM base
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        HOUR(played_at) AS played_at_hour,
        COALESCE(device, 'UNKNOWN') AS device_or_unknown
    FROM labeled
    WHERE played_at < TIMESTAMP '2025-02-18 13:00:00'
),
with_parent AS (
    -- Attach track attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.is_skipped,
        s.source_device_label,
        s.played_at_label_clean,
        s.played_at_date,
        s.played_at_hour,
        t.artist_id AS track_artist_id
    FROM annotated s
    JOIN tracks t ON t.id = s.track_id
),
unique_rows AS (
    -- Keep the earliest row per track_artist_id, listener_id
    SELECT
        id,
        listener_id,
        track_id,
        is_skipped,
        source_device_label,
        played_at_label_clean,
        played_at_date,
        played_at_hour,
        track_artist_id
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY track_artist_id, listener_id ORDER BY played_at_hour, id) AS row_num
        FROM with_parent
    ) ranked
    WHERE row_num = 1
)
SELECT
    id,
    track_id,
    is_skipped,
    played_at_date,
    track_artist_id,
    UPPER(TRIM(played_at_label_clean)) AS played_at_label_clean_clean,
    ABS(played_at_hour - 12) AS played_at_hour_distance
FROM unique_rows
ORDER BY track_id
