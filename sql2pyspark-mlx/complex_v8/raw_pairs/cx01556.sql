WITH prepared_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source,
        CASE WHEN device IN ('tv', 'desktop') THEN 'group_a' WHEN device = 'phone' THEN 'group_b' ELSE 'other' END AS device_group,
        QUARTER(played_at) AS played_at_quarter
    FROM plays
    WHERE device = 'car'
      AND source = 'radio'
),
with_details_track AS (
    -- Enrich with dimension data
    SELECT s.*, t.is_explicit AS track_is_explicit, l.plan AS listener_plan
    FROM prepared_plays s
    LEFT JOIN tracks t ON t.id = s.track_id AND UPPER(TRIM(t.title)) LIKE 'E%'
    LEFT JOIN listeners l ON l.id = s.listener_id
),
combined_details AS (
    -- Attach listener attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.device,
        s.is_skipped,
        s.source,
        s.device_group,
        s.played_at_quarter,
        s.track_is_explicit,
        s.listener_plan,
        l.interests AS listener_interests,
        l.username AS listener_username,
        l.birth_year AS listener_birth_year
    FROM with_details_track s
    JOIN listeners l ON l.id = s.listener_id
)
SELECT *
FROM (
    SELECT listener_plan, device_group, listener_birth_year
    FROM combined_details
) src
PIVOT (
    AVG(listener_birth_year)
    FOR device_group IN ('group_a', 'other', 'group_b')
)
ORDER BY listener_plan
