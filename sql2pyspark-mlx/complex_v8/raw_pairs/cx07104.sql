WITH prepared AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source
    FROM plays
    WHERE MONTH(played_at) IN (2, 3)
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN device IN ('car', 'desktop') THEN 'group_a' WHEN device = 'phone' THEN 'group_b' ELSE 'other' END AS device_group,
        INITCAP(TRIM(source)) AS source_clean,
        IF(is_skipped, 'yes', 'no') AS is_skipped_label
    FROM prepared
    WHERE ms_played < 221269
)
SELECT *
FROM (
    SELECT is_skipped_label, device_group, ms_played
    FROM labeled
) src
PIVOT (
    MAX(ms_played)
    FOR device_group IN ('other', 'group_a', 'group_b')
)
