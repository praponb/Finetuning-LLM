WITH cleaned AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        IF(is_skipped, 'yes', 'no') AS is_skipped_label,
        CONCAT_WS('-', source, device) AS source_device_label,
        CONCAT(source, '#', CAST(id AS STRING)) AS source_ref
    FROM plays
),
unique_rows AS (
    -- Keep the earliest row per source, is_skipped_label
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        is_skipped_label,
        source_device_label,
        source_ref
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY source, is_skipped_label ORDER BY played_at, id) AS recency_rank
        FROM cleaned
    ) ranked
    WHERE recency_rank = 1
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source,
        is_skipped_label,
        source_ref,
        COUNT(*) OVER (PARTITION BY source, device) AS rows_in_source_device,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER w_source_device, 0), 4) AS ms_played_share_of_source_device,
        RANK() OVER w_device_by_played_at AS played_at_rank
    FROM unique_rows
    WINDOW w_source_device AS (PARTITION BY source, device), w_device_by_played_at AS (PARTITION BY device ORDER BY played_at, id)
)
-- Subtotals with ROLLUP
SELECT
    listener_id,
    is_skipped,
    GROUPING(listener_id) AS is_total_listener_id,
    SUM(played_at_rank) FILTER (WHERE source NOT IN ('radio', 'album')) AS played_at_rank_source_not_in_radio_album,
    MIN(played_at) AS first_played_at,
    COUNT(*) AS cnt
FROM windowed
GROUP BY ROLLUP(listener_id, is_skipped)
ORDER BY listener_id NULLS LAST, is_skipped NULLS LAST
