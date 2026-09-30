WITH valid AS (
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
        COALESCE(device, 'UNKNOWN') AS device_or_unknown,
        LOWER(REGEXP_REPLACE(TRIM(device), ' ', '_')) AS device_slug,
        HOUR(played_at) AS played_at_hour
    FROM plays
),
track_id_stats AS (
    -- Avg ms_played per track_id
    SELECT track_id, AVG(ms_played) AS avg_ms_played
    FROM valid
    GROUP BY track_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_ms_played,
        ROUND(s.ms_played - g.avg_ms_played, 2) AS ms_played_minus_avg
    FROM valid s
    JOIN track_id_stats g ON g.track_id = s.track_id
    WHERE s.ms_played > g.avg_ms_played
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (ORDER BY played_at_hour), 3) AS played_at_hour_percent_rank,
        ROUND(AVG(avg_ms_played) OVER (PARTITION BY is_skipped ORDER BY played_at_hour, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS avg_ms_played_avg_4,
        ROW_NUMBER() OVER w_source_by_played_at_hour AS rn,
        LEAD(played_at) OVER (PARTITION BY is_skipped ORDER BY played_at_hour, id) AS next_played_at,
        LAST_VALUE(played_at_hour) IGNORE NULLS OVER (PARTITION BY source ORDER BY played_at_hour ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_played_at_hour,
        FIRST_VALUE(avg_ms_played) IGNORE NULLS OVER (ORDER BY played_at_hour, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_avg_ms_played
    FROM compared
    WINDOW w_source_by_played_at_hour AS (PARTITION BY source ORDER BY played_at_hour ASC, id)
)
SELECT
    id,
    track_id,
    ms_played,
    is_skipped,
    source,
    device_or_unknown,
    device_slug,
    played_at_hour,
    avg_ms_played,
    ms_played_minus_avg,
    played_at_hour_percent_rank,
    avg_ms_played_avg_4,
    rn,
    next_played_at,
    last_known_played_at_hour,
    first_known_avg_ms_played,
    LEAST(avg_ms_played, avg_ms_played_avg_4) AS least_avg_ms_played_avg_ms_played_avg_4,
    FLOOR(ms_played_minus_avg / 100000) * 100000 AS ms_played_minus_avg_bucket
FROM windowed
ORDER BY track_id DESC
