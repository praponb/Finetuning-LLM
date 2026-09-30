WITH filtered_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source,
        LENGTH(TRIM(source)) AS source_len,
        CONCAT(device, '#', CAST(id AS STRING)) AS device_ref,
        CONCAT_WS(': ', source, device) AS source_device_label
    FROM plays
    WHERE listener_id <= 12
),
summary AS (
    -- Group-level totals
    SELECT
        device,
        source_len,
        source,
        SUM(source_len) AS total_source_len,
        COUNT(*) AS row_count,
        ARRAY_SORT(COLLECT_SET(source)) AS source_list,
        ROUND(100 * COUNT(CASE WHEN device = 'desktop' THEN 1 END) / COUNT(*), 1) AS pct_device_desktop,
        ROUND(100 * COUNT(CASE WHEN device IS NOT NULL THEN 1 END) / COUNT(*), 1) AS pct_device_is_not_null
    FROM filtered_plays
    GROUP BY 1, 2, 3
),
exploded AS (
    -- One row per source_list_item
    SELECT
        s.device,
        s.source_len,
        s.source,
        s.total_source_len,
        s.row_count,
        s.pct_device_desktop,
        s.pct_device_is_not_null,
        t.source_list_item
    FROM summary s
    LATERAL VIEW EXPLODE(s.source_list) t AS source_list_item
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM exploded
    WHERE source_len > (SELECT AVG(source_len) FROM exploded)
)
-- Final projection
SELECT
    device,
    source_len,
    source,
    row_count,
    pct_device_desktop,
    source_list_item
FROM screened
ORDER BY source_list_item
