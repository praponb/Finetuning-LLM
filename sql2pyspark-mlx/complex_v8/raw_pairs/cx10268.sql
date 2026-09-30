WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source,
        HOUR(played_at) AS played_at_hour,
        LOWER(REGEXP_REPLACE(TRIM(device), ' ', '_')) AS device_slug,
        CASE WHEN source IN ('radio', 'playlist') THEN 'group_a' WHEN source = 'album' THEN 'group_b' ELSE 'other' END AS source_group
    FROM plays
    WHERE source <> 'radio'
),
with_details AS (
    -- Attach listener attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.device,
        s.is_skipped,
        s.source,
        s.device_slug,
        s.source_group,
        l.interests AS listener_interests,
        l.plan AS listener_plan,
        l.username AS listener_username
    FROM recent s
    JOIN listeners l ON l.id = s.listener_id
),
unique_rows AS (
    -- Keep the earliest row per source_group
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source,
        device_slug,
        source_group,
        listener_interests,
        listener_plan,
        listener_username
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY source_group ORDER BY played_at, id) AS row_num
        FROM with_details
    ) ranked
    WHERE row_num = 1
),
with_details_2 AS (
    -- Join listeners
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.device,
        s.is_skipped,
        s.device_slug,
        s.source_group,
        s.listener_interests,
        s.listener_plan,
        s.listener_username,
        l.birth_year AS listener_birth_year
    FROM unique_rows s
    LEFT JOIN listeners l ON l.id = s.listener_id
)
SELECT
    listener_id,
    track_id,
    listener_plan,
    COUNT(*) AS row_count,
    MIN(listener_birth_year) AS min_listener_birth_year,
    COUNT(DISTINCT device) AS distinct_devices
FROM with_details_2
GROUP BY GROUPING SETS ((listener_id, track_id, listener_plan), (listener_id), ())
