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
        DATE_FORMAT(played_at, 'yyyy-MM') AS played_at_label,
        LOWER(REGEXP_REPLACE(TRIM(source), ' ', '_')) AS source_slug
    FROM plays
    WHERE source = 'search'
      AND ms_played >= 51883
),
summary AS (
    -- Group-level totals
    SELECT
        device,
        source,
        MIN(ms_played) AS min_ms_played,
        SUM(CASE WHEN source = 'playlist' THEN ms_played ELSE 0 END) AS ms_played_source_playlist,
        SUM(CASE WHEN source NOT IN ('playlist', 'search') THEN 1 ELSE 0 END) AS n_source_not_in_playlist_searc,
        SUM(CASE WHEN NOT is_skipped THEN ms_played ELSE 0 END) AS ms_played_not_is_skipped,
        COUNT(*) AS row_count
    FROM filtered_plays
    GROUP BY 1, 2
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN device IN ('car', 'tv') THEN 'group_a' WHEN device = 'phone' THEN 'group_b' ELSE 'other' END AS device_group
    FROM summary
)
SELECT DISTINCT source
FROM derived
WHERE source = 'radio'
ORDER BY source
