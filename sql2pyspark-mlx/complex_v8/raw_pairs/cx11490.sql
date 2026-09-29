WITH recent_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source,
        DATE_TRUNC('QUARTER', played_at) AS played_at_quarter
    FROM plays
    WHERE HOUR(played_at) BETWEEN 7 AND 11
),
stats AS (
    -- Group-level totals
    SELECT
        CASE WHEN ms_played >= 301865 THEN 'large' WHEN ms_played >= 99376 THEN 'medium' ELSE 'small' END AS ms_played_tier,
        COUNT(*) AS cnt,
        COUNT(DISTINCT id) AS distinct_ids,
        SUM(CASE WHEN source IN ('search', 'playlist', 'album') THEN 1 ELSE 0 END) AS n_source_in_search_playlist_al
    FROM recent_plays
    GROUP BY 1
),
tagged AS (
    -- Add calculated columns
    SELECT *, UPPER(TRIM(ms_played_tier)) AS ms_played_tier_clean
    FROM stats
)
-- Final projection
SELECT
    ms_played_tier,
    cnt,
    n_source_in_search_playlist_al,
    ms_played_tier_clean,
    LOWER(REGEXP_REPLACE(TRIM(ms_played_tier), ' ', '_')) AS ms_played_tier_slug
FROM tagged
ORDER BY ms_played_tier, cnt
