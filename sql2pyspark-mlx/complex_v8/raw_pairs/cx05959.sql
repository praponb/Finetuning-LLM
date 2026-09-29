WITH scoped AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        is_skipped,
        source,
        LENGTH(TRIM(device)) AS device_len
    FROM plays
    WHERE ms_played <= 282311
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM scoped
    WHERE device_len > (SELECT AVG(device_len) FROM scoped)
),
listener_id_stats AS (
    -- Median device_len per listener_id
    SELECT listener_id, PERCENTILE_APPROX(device_len, 0.5) AS median_device_len
    FROM with_activity
    GROUP BY listener_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_device_len,
        ROUND(s.device_len - g.median_device_len, 2) AS device_len_minus_median
    FROM with_activity s
    JOIN listener_id_stats g ON g.listener_id = s.listener_id
    WHERE s.device_len >= g.median_device_len
),
without_matches AS (
    -- Compare against the overall max
    SELECT *
    FROM above_average
    WHERE device_len_minus_median < (SELECT MAX(device_len_minus_median) FROM above_average)
)
SELECT
    listener_id,
    device_len,
    source,
    GROUPING(listener_id) AS is_total_listener_id,
    COUNT(DISTINCT listener_id) AS distinct_listener_ids,
    PERCENTILE_APPROX(median_device_len, 0.5) AS median_median_device_len,
    SUM(CASE WHEN source = 'album' THEN 1 ELSE 0 END) AS n_source_album
FROM without_matches
GROUP BY GROUPING SETS ((listener_id, device_len, source), (listener_id), (device_len), ())
