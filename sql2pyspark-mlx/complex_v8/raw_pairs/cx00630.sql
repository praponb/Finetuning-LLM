WITH prepared AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        CASE WHEN ms_played >= 257789 THEN 'HIGH' WHEN ms_played >= 97825 THEN 'MEDIUM' ELSE 'LOW' END AS ms_played_tier
    FROM plays
    WHERE source = 'album'
      AND MONTH(played_at) IN (2, 3)
),
joined_track AS (
    -- Join tracks
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.device,
        s.is_skipped,
        s.source,
        t.title AS track_title,
        t.duration_sec AS track_duration_sec
    FROM prepared s
    JOIN tracks t ON t.id = s.track_id
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTH_VALUE(ms_played, 2) OVER (PARTITION BY listener_id, device ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ms_played,
        LAG(track_duration_sec, 3) OVER (PARTITION BY listener_id, device ORDER BY ms_played, id) AS prev3_track_duration_sec
    FROM joined_track
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        COALESCE(track_duration_sec, 0) AS track_duration_sec_filled,
        COALESCE(prev3_track_duration_sec, 0) AS prev3_track_duration_sec_filled,
        CASE WHEN device IN ('phone', 'tv') THEN 'group_a' WHEN device = 'speaker' THEN 'group_b' ELSE 'other' END AS device_group
    FROM metrics
)
SELECT
    track_id,
    is_skipped,
    MIN(track_duration_sec) AS min_track_duration_sec
FROM labeled
GROUP BY ROLLUP(track_id, is_skipped)
