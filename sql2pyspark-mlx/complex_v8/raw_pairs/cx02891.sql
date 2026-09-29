WITH valid_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        source,
        CONCAT_WS('|', source, device) AS source_device_label,
        SUBSTRING(TRIM(source), 1, 3) AS source_prefix,
        CONCAT_WS('|', device, source) AS device_source_label
    FROM plays
    WHERE device = 'desktop'
      AND listener_id >= 6
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(ms_played) OVER (PARTITION BY track_id), 2) AS track_id_avg_ms_played,
        LAG(played_at) OVER (ORDER BY played_at DESC, id) AS prev_played_at,
        SIZE(COLLECT_SET(listener_id) OVER (PARTITION BY track_id)) AS distinct_listener_id_in_track_id,
        DENSE_RANK() OVER (ORDER BY played_at DESC, id) AS played_at_dense_rank
    FROM valid_plays
)
SELECT
    device,
    distinct_listener_id_in_track_id,
    GROUPING(device) AS is_total_device,
    ROUND(SUM(track_id_avg_ms_played) / NULLIF(SUM(ms_played), 0), 3) AS track_id_avg_ms_played_to_ms_played_ratio
FROM ranked
GROUP BY CUBE(device, distinct_listener_id_in_track_id)
