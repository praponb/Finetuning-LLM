WITH valid_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        CASE WHEN device IN ('car', 'desktop') THEN 'group_a' WHEN device = 'speaker' THEN 'group_b' ELSE 'other' END AS device_group
    FROM plays
    WHERE played_at >= TIMESTAMP '2025-01-19 11:00:00'
),
unioned AS (
    -- Stack the branches
    SELECT source AS category, track_id AS ref_id, played_at AS event_time
    FROM valid_plays
    WHERE device_group <> 'other'
      AND listener_id <= 17
    UNION ALL
    SELECT device AS category, listener_id AS ref_id, played_at AS event_time
    FROM plays
    WHERE track_id <= 15
)
SELECT category, COUNT(*) AS n_rows, SUM(ref_id) AS total_ref_id, MAX(event_time) AS latest_event_time
FROM unioned
GROUP BY category
ORDER BY category
