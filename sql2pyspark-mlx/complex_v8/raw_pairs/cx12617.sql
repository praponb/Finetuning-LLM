WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        SUBSTRING(TRIM(source), 1, 3) AS source_prefix,
        COALESCE(ms_played, 0) AS ms_played_filled,
        SUBSTRING(TRIM(device), 1, 2) AS device_prefix
    FROM plays
    WHERE ms_played >= 157900
      AND listener_id IS NOT NULL
),
listener_id_stats AS (
    -- Avg ms_played_filled per listener_id
    SELECT listener_id, AVG(ms_played_filled) AS avg_ms_played_filled
    FROM valid
    GROUP BY listener_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_ms_played_filled,
        ROUND(s.ms_played_filled - g.avg_ms_played_filled, 2) AS ms_played_filled_minus_avg
    FROM valid s
    JOIN listener_id_stats g ON g.listener_id = s.listener_id
    WHERE s.ms_played_filled > g.avg_ms_played_filled
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        played_at,
        device,
        is_skipped,
        source_prefix,
        ms_played_filled,
        device_prefix,
        avg_ms_played_filled,
        ms_played_filled_minus_avg,
        LENGTH(TRIM(source_prefix)) AS source_prefix_len
    FROM above_average
)
SELECT
    source_prefix_len,
    device,
    ROUND(100 * COUNT(CASE WHEN NOT is_skipped THEN 1 END) / COUNT(*), 1) AS pct_not_is_skipped
FROM annotated
GROUP BY GROUPING SETS ((source_prefix_len, device), (source_prefix_len), (device), ())
