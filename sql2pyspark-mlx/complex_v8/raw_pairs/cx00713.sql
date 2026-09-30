WITH prepared AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        device,
        is_skipped,
        source,
        CASE WHEN ms_played >= 308104 THEN 'A' WHEN ms_played >= 73335 THEN 'B' ELSE 'C' END AS ms_played_tier
    FROM plays
    WHERE ms_played >= 108979
),
with_details AS (
    -- Attach listener attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.device,
        s.is_skipped,
        s.source,
        s.ms_played_tier,
        l.birth_year AS listener_birth_year,
        l.interests AS listener_interests
    FROM prepared s
    JOIN listeners l ON l.id = s.listener_id
    WHERE SIZE(l.interests) >= 1
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT_WS('|', ms_played_tier, device) AS ms_played_tier_device_label,
        CONCAT_WS(': ', device, source) AS device_source_label,
        CASE WHEN ms_played_tier IN ('C', 'A') THEN 'group_a' WHEN ms_played_tier = 'B' THEN 'group_b' ELSE 'other' END AS ms_played_tier_group
    FROM with_details
)
-- Subtotals with GROUPING SETS
SELECT
    device,
    ms_played_tier_group,
    GROUPING_ID() AS gid,
    COUNT(DISTINCT track_id) AS distinct_track_ids
FROM scored
GROUP BY GROUPING SETS ((device, ms_played_tier_group), (device), ())
