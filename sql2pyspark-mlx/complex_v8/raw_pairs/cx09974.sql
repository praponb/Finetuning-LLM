WITH cleaned_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        bpm,
        moods
    FROM tracks
    WHERE UPPER(TRIM(title)) LIKE 'S%'
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        SUBSTRING(TRIM(title), 1, 4) AS title_prefix,
        GREATEST(bpm, duration_sec) AS greatest_bpm_duration_sec,
        CASE WHEN bpm >= 147 THEN 'A' WHEN bpm >= 85 THEN 'B' ELSE 'C' END AS bpm_tier
    FROM cleaned_tracks
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        bpm,
        bpm_tier,
        CASE WHEN bpm_tier IN ('B', 'A') THEN 'group_a' WHEN bpm_tier = 'C' THEN 'group_b' ELSE 'other' END AS bpm_tier_group,
        CASE WHEN bpm_tier IN ('A', 'C') THEN 'group_a' WHEN bpm_tier = 'B' THEN 'group_b' ELSE 'other' END AS bpm_tier_group_2
    FROM tagged
)
SELECT
    id,
    artist_id,
    title,
    duration_sec,
    bpm,
    bpm_tier_group,
    bpm_tier_group_2,
    FLOOR(duration_sec / 50) * 50 AS duration_sec_bucket,
    CASE WHEN duration_sec >= 352 THEN 'large' WHEN duration_sec >= 183 THEN 'medium' ELSE 'small' END AS duration_sec_tier
FROM annotated
ORDER BY bpm_tier_group_2, duration_sec_bucket DESC NULLS FIRST, id
LIMIT 5
