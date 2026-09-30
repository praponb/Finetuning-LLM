WITH filtered_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        device,
        is_skipped,
        source,
        CONCAT_WS('|', source, device) AS source_device_label
    FROM plays
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN is_skipped THEN 1 ELSE 0 END AS is_skipped_flag,
        CASE WHEN source IN ('radio', 'playlist') THEN 'group_a' WHEN source = 'search' THEN 'group_b' ELSE 'other' END AS source_group
    FROM filtered_plays
),
latest AS (
    -- Keep the latest row per listener_id, device
    SELECT
        id,
        listener_id,
        track_id,
        device,
        is_skipped,
        source,
        source_device_label,
        is_skipped_flag,
        source_group
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY listener_id, device ORDER BY is_skipped_flag DESC, id DESC) AS row_num
        FROM labeled
    ) ranked
    WHERE row_num = 1
)
-- Final projection
SELECT
    id,
    listener_id,
    device,
    is_skipped,
    source_device_label,
    source_group,
    LENGTH(TRIM(device)) AS device_len
FROM latest
WHERE NOT is_skipped
ORDER BY id NULLS LAST
