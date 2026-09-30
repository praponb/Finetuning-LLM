WITH recent AS (
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
        SUBSTRING(TRIM(device), 1, 4) AS device_prefix,
        ABS(ms_played - 223828) AS ms_played_distance,
        DATE_FORMAT(played_at, 'yyyy-MM-dd HH:00') AS played_at_label
    FROM plays
    WHERE listener_id IS NOT NULL
      AND source NOT IN ('playlist', 'album')
),
with_details_listener AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.device,
        s.is_skipped,
        s.source,
        s.device_prefix,
        s.ms_played_distance,
        s.played_at_label,
        l.interests AS listener_interests
    FROM recent s
    LEFT JOIN listeners l ON l.id = s.listener_id
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        FIRST_VALUE(ms_played_distance) IGNORE NULLS OVER (PARTITION BY source, is_skipped ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_ms_played_distance,
        LAG(ms_played) OVER (PARTITION BY source, is_skipped ORDER BY ms_played, id) AS prev_ms_played
    FROM with_details_listener
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device_prefix,
        played_at_label,
        first_known_ms_played_distance,
        prev_ms_played,
        NTH_VALUE(ms_played_distance, 2) OVER (PARTITION BY is_skipped, device ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ms_played_distance,
        LAG(ms_played, 1, 0) OVER w_is_skipped_device_by_ms_play_2 AS prev_ms_played_2
    FROM ranked
    WINDOW w_is_skipped_device_by_ms_play_2 AS (PARTITION BY is_skipped, device ORDER BY ms_played, id)
),
listener_id_stats AS (
    -- Max prev_ms_played_2 per listener_id
    SELECT listener_id, MAX(prev_ms_played_2) AS max_prev_ms_played_2
    FROM windowed
    GROUP BY listener_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_prev_ms_played_2,
        ROUND(s.prev_ms_played_2 - g.max_prev_ms_played_2, 2) AS prev_ms_played_2_minus_max
    FROM windowed s
    JOIN listener_id_stats g ON g.listener_id = s.listener_id
    WHERE s.prev_ms_played_2 = g.max_prev_ms_played_2
),
track_id_stats AS (
    -- Median max_prev_ms_played_2 per track_id
    SELECT track_id, PERCENTILE_APPROX(max_prev_ms_played_2, 0.5) AS median_max_prev_ms_played_2
    FROM vs_group
    GROUP BY track_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_max_prev_ms_played_2,
        ROUND(s.max_prev_ms_played_2 - g.median_max_prev_ms_played_2, 2) AS max_prev_ms_played_2_minus_median
    FROM vs_group s
    JOIN track_id_stats g ON g.track_id = s.track_id
    WHERE s.max_prev_ms_played_2 >= g.median_max_prev_ms_played_2
),
listener_id_stats_2 AS (
    -- Avg ms_played per listener_id
    SELECT listener_id, AVG(ms_played) AS avg_ms_played
    FROM above_average
    GROUP BY listener_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_ms_played,
        ROUND(s.ms_played - g.avg_ms_played, 2) AS ms_played_minus_avg
    FROM above_average s
    JOIN listener_id_stats_2 g ON g.listener_id = s.listener_id
    WHERE s.ms_played > g.avg_ms_played
)
SELECT
    id,
    listener_id,
    track_id,
    played_at_label,
    prev_ms_played,
    second_ms_played_distance,
    prev_ms_played_2,
    max_prev_ms_played_2,
    prev_ms_played_2_minus_max,
    max_prev_ms_played_2_minus_median,
    ms_played_minus_avg,
    SUBSTRING(TRIM(device_prefix), 1, 4) AS device_prefix_prefix
FROM compared
ORDER BY prev_ms_played_2_minus_max DESC, id DESC
