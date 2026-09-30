WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        CONCAT_WS('-', device, source) AS device_source_label
    FROM plays
    WHERE is_skipped = FALSE
),
latest AS (
    -- Keep the latest row per device, source
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        device_source_label
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY device, source ORDER BY played_at DESC, id DESC) AS row_num
        FROM recent
    ) ranked
    WHERE row_num = 1
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        CASE WHEN source IN ('album', 'search') THEN 'group_a' WHEN source = 'radio' THEN 'group_b' ELSE 'other' END AS source_group
    FROM latest
)
SELECT
    id,
    listener_id,
    track_id,
    ms_played,
    device,
    source,
    source_group,
    COALESCE(ms_played, 0) AS ms_played_filled
FROM scored
WHERE is_skipped = TRUE
ORDER BY ms_played_filled DESC
