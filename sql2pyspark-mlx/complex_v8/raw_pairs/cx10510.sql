WITH valid AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        source,
        DATE_FORMAT(played_at, 'yyyy-MM-dd') AS played_at_label,
        ABS(ms_played - 178909) AS ms_played_distance,
        CONCAT_WS('-', device, source) AS device_source_label
    FROM plays
    WHERE device = 'speaker'
),
grouped AS (
    -- Summarise per group
    SELECT
        device,
        source,
        track_id,
        COUNT(*) AS n_rows,
        MAX(ms_played) AS max_ms_played,
        COUNT(*) FILTER (WHERE device = 'speaker') AS n_device_speaker,
        ROUND(STDDEV(ms_played), 2) AS stddev_ms_played,
        ROUND(SUM(ms_played_distance) / NULLIF(SUM(ms_played), 0), 3) AS ms_played_distance_to_ms_played_ratio
    FROM valid
    WHERE device IS NOT NULL
    GROUP BY 1, 2, 3
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT n_rows, device, stddev_ms_played
    FROM grouped
) src
PIVOT (
    SUM(stddev_ms_played)
    FOR device IN ('speaker', 'tv', 'car', 'desktop')
)
ORDER BY n_rows
