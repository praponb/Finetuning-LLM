WITH filtered_plays AS (
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
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket
    FROM plays
    WHERE ms_played > 56848
      AND is_skipped = TRUE
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        ms_played_bucket,
        ms_played - ms_played_bucket AS ms_played_minus_ms_played_bucket,
        COALESCE(device, 'UNKNOWN') AS device_or_unknown,
        CASE WHEN source IN ('search', 'playlist') THEN 'group_a' WHEN source = 'album' THEN 'group_b' ELSE 'other' END AS source_group
    FROM filtered_plays
    WHERE device IN ('desktop', 'speaker')
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(ms_played_bucket, 0) AS ms_played_bucket_filled,
        CASE WHEN ms_played_bucket >= 242000 THEN 'HIGH' WHEN ms_played_bucket >= 123500 THEN 'MEDIUM' ELSE 'LOW' END AS ms_played_bucket_tier
    FROM annotated
),
with_activity AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM tagged s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.listener_id
              AND p.device <> 'tv'
        )
)
-- Subtotals with ROLLUP
SELECT
    source_group,
    source,
    track_id,
    COUNT(*) FILTER (WHERE source IN ('search', 'playlist')) AS n_source_in_search_playlist
FROM with_activity
GROUP BY ROLLUP(source_group, source, track_id)
ORDER BY source_group NULLS LAST, source NULLS LAST, track_id NULLS LAST
