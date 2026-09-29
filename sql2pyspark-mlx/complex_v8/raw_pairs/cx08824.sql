WITH valid AS (
    -- Filter plays
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
scored AS (
    -- Add calculated columns
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        IF(is_skipped, 'yes', 'no') AS is_skipped_label,
        CASE WHEN source IN ('search', 'playlist') THEN 'group_a' WHEN source = 'radio' THEN 'group_b' ELSE 'other' END AS source_group
    FROM valid
    WHERE source = 'playlist'
)
SELECT
    listener_id,
    is_skipped_label,
    COUNT(*) AS row_count,
    COUNT(DISTINCT listener_id) AS distinct_listener_ids
FROM scored
GROUP BY GROUPING SETS ((listener_id, is_skipped_label), (listener_id), ())
ORDER BY listener_id NULLS LAST, is_skipped_label NULLS LAST
