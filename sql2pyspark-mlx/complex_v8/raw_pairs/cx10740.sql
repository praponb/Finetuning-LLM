WITH recent_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        source,
        played_at + INTERVAL 15 MINUTES AS played_at_plus_15m,
        DATE_FORMAT(played_at, 'yyyy-MM-dd') AS played_at_label
    FROM plays
    WHERE track_id IS NOT NULL
),
source_stats AS (
    -- Avg ms_played per source
    SELECT source, AVG(ms_played) AS avg_ms_played
    FROM recent_plays
    GROUP BY source
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_ms_played,
        ROUND(s.ms_played - g.avg_ms_played, 2) AS ms_played_minus_avg
    FROM recent_plays s
    JOIN source_stats g ON g.source = s.source
    WHERE s.ms_played > g.avg_ms_played
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTH_VALUE(ms_played, 2) OVER (ORDER BY ms_played_minus_avg ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ms_played,
        MAX(ms_played_minus_avg) OVER (PARTITION BY device ORDER BY avg_ms_played, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS ms_played_minus_avg_max_6_prior,
        ROUND(AVG(avg_ms_played) OVER (PARTITION BY device ORDER BY avg_ms_played, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS avg_ms_played_avg_4,
        ROUND(AVG(ms_played) OVER (PARTITION BY device), 2) AS device_avg_ms_played
    FROM above_average
)
-- Final projection
SELECT
    id,
    track_id,
    ms_played,
    device,
    source,
    played_at_plus_15m,
    played_at_label,
    ms_played_minus_avg,
    second_ms_played,
    ms_played_minus_avg_max_6_prior,
    avg_ms_played_avg_4,
    device_avg_ms_played,
    LENGTH(TRIM(source)) AS source_len
FROM metrics
