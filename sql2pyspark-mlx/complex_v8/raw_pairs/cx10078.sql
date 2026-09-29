WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped
    FROM plays
    WHERE track_id >= 5
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        listener_id,
        ms_played,
        device,
        CASE WHEN ms_played >= 245815 THEN 'A' WHEN ms_played >= 129336 THEN 'B' ELSE 'C' END AS ms_played_tier,
        UPPER(TRIM(device)) AS device_clean
    FROM recent
),
combined_details AS (
    -- Join listeners
    SELECT
        s.id,
        s.listener_id,
        s.ms_played,
        s.device,
        s.ms_played_tier,
        s.device_clean,
        l.plan AS listener_plan,
        l.interests AS listener_interests,
        l.username AS listener_username
    FROM derived s
    JOIN listeners l ON l.id = s.listener_id
)
-- Final projection
SELECT
    id,
    ms_played,
    device,
    device_clean,
    listener_plan,
    listener_interests,
    listener_username,
    UPPER(TRIM(listener_plan)) AS listener_plan_clean,
    CASE WHEN device IN ('tv', 'desktop') THEN 'group_a' WHEN device = 'car' THEN 'group_b' ELSE 'other' END AS device_group
FROM combined_details
