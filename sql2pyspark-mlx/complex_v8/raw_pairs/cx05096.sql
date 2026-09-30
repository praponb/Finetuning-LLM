WITH valid_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        ABS(ms_played - 183186) AS ms_played_distance
    FROM plays
    WHERE track_id >= 15
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        ABS(ms_played - 183186) AS ms_played_distance_2,
        CASE WHEN device IN ('tv', 'phone') THEN 'group_a' WHEN device = 'car' THEN 'group_b' ELSE 'other' END AS device_group
    FROM valid_plays
)
-- Final projection
SELECT
    id,
    listener_id,
    track_id,
    ms_played,
    device,
    is_skipped,
    source,
    ms_played_distance,
    ms_played_distance_2,
    device_group
FROM labeled
WHERE ms_played < 236512
ORDER BY ms_played_distance, device_group DESC
