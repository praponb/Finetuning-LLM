WITH prepared AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source
    FROM plays
    WHERE HOUR(played_at) BETWEEN 6 AND 9
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN device IN ('desktop', 'phone') THEN 'group_a' WHEN device = 'speaker' THEN 'group_b' ELSE 'other' END AS device_group,
        CONCAT(device, '#', CAST(listener_id AS STRING)) AS device_ref,
        DAYOFWEEK(played_at) AS played_at_dayofweek
    FROM prepared
    WHERE track_id <= 20
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        source,
        device_group,
        played_at_dayofweek,
        LAST_VALUE(ms_played) IGNORE NULLS OVER (ORDER BY played_at_dayofweek ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_ms_played,
        NTILE(3) OVER w_all_by_played_at_dayofweek AS played_at_dayofweek_tercile
    FROM annotated
    WINDOW w_all_by_played_at_dayofweek AS (ORDER BY played_at_dayofweek ASC, id)
)
SELECT
    device_group,
    track_id,
    listener_id,
    GROUPING_ID() AS grouping_level,
    SUM(ms_played) AS total_ms_played
FROM windowed
GROUP BY GROUPING SETS ((device_group, track_id, listener_id), (device_group), (track_id))
ORDER BY device_group NULLS LAST, track_id NULLS LAST, listener_id NULLS LAST
