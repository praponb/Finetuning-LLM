WITH valid AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        is_skipped,
        source,
        CASE WHEN source IN ('radio', 'album') THEN 'group_a' WHEN source = 'search' THEN 'group_b' ELSE 'other' END AS source_group
    FROM plays
    WHERE source = 'search'
),
with_activity AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.track_id
              AND p.source IN ('radio', 'playlist', 'search')
        )
      AND s.track_id IS NOT NULL
)
SELECT
    id,
    listener_id,
    is_skipped,
    source,
    source_group,
    COALESCE(source, 'UNKNOWN') AS source_or_unknown,
    COALESCE(source, 'UNKNOWN') AS source_or_unknown_2
FROM with_activity
ORDER BY source_or_unknown NULLS LAST, source
