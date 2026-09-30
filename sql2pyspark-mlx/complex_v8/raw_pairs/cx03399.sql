WITH filtered_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm,
        CONCAT(title, '#', CAST(bpm AS STRING)) AS title_ref,
        ABS(duration_sec - 312) AS duration_sec_distance,
        CASE WHEN duration_sec >= 357 THEN 'A' WHEN duration_sec >= 209 THEN 'B' ELSE 'C' END AS duration_sec_tier
    FROM tracks
    WHERE artist_id IS NOT NULL
),
enriched AS (
    -- Join artists
    SELECT s.*, a.is_verified AS artist_is_verified
    FROM filtered_tracks s
    LEFT JOIN artists a ON a.id = s.artist_id
),
enriched_2 AS (
    -- Derive helper columns
    SELECT
        id,
        release_date,
        bpm,
        title_ref,
        duration_sec_distance,
        artist_is_verified,
        FLOOR(duration_sec / 50) * 50 AS duration_sec_bucket,
        CASE WHEN duration_sec_tier IN ('B', 'C') THEN 'group_a' WHEN duration_sec_tier = 'A' THEN 'group_b' ELSE 'other' END AS duration_sec_tier_group,
        COALESCE(duration_sec, 0) AS duration_sec_filled
    FROM enriched
)
SELECT
    artist_is_verified,
    duration_sec_tier_group,
    GROUPING_ID() AS grouping_level,
    COUNT(*) AS row_count,
    SUM(duration_sec_bucket) AS total_duration_sec_bucket,
    MAX(release_date) AS last_release_date
FROM enriched_2
GROUP BY ROLLUP(artist_is_verified, duration_sec_tier_group)
ORDER BY artist_is_verified NULLS LAST, duration_sec_tier_group NULLS LAST
