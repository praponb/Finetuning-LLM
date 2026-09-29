WITH scoped_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        source,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket,
        LOWER(REGEXP_REPLACE(TRIM(source), ' ', '_')) AS source_slug
    FROM plays
    WHERE ms_played BETWEEN 112786 AND 225733
),
with_details_listener AS (
    -- Join listeners
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.device,
        s.source_slug,
        l.interests AS listener_interests
    FROM scoped_plays s
    JOIN listeners l ON l.id = s.listener_id
)
-- Subtotals with CUBE
SELECT
    COALESCE(device, 'ALL') AS device,
    track_id,
    GROUPING_ID() AS gid,
    ARRAY_SORT(COLLECT_SET(device)) AS device_list,
    COUNT(*) AS n_rows,
    COUNT(DISTINCT id) AS distinct_ids
FROM with_details_listener
GROUP BY CUBE(device, track_id)
ORDER BY device NULLS LAST, track_id NULLS LAST
