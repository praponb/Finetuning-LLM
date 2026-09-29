WITH filtered_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped
    FROM plays
    WHERE device IS NOT NULL
)
-- Subtotals with ROLLUP
SELECT
    COALESCE(device, 'ALL') AS device,
    track_id,
    is_skipped,
    ARRAY_SORT(COLLECT_SET(device)) AS device_list
FROM filtered_plays
GROUP BY ROLLUP(device, track_id, is_skipped)
ORDER BY device NULLS LAST, track_id NULLS LAST, is_skipped NULLS LAST
