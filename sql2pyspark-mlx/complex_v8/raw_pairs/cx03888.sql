WITH scoped_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        device,
        is_skipped,
        source,
        CONCAT_WS(': ', device, source) AS device_source_label
    FROM plays
    WHERE device IN ('tv', 'phone')
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        track_id,
        is_skipped,
        device_source_label,
        CASE WHEN source IN ('playlist', 'album') THEN 'group_a' WHEN source = 'search' THEN 'group_b' ELSE 'other' END AS source_group,
        CASE WHEN source IN ('playlist', 'album') THEN 'group_a' WHEN source = 'search' THEN 'group_b' ELSE 'other' END AS source_group_2
    FROM scoped_plays
)
SELECT
    source_group,
    track_id,
    is_skipped,
    COUNT(*) AS n_rows
FROM scored
GROUP BY ROLLUP(source_group, track_id, is_skipped)
ORDER BY source_group NULLS LAST, track_id NULLS LAST, is_skipped NULLS LAST
