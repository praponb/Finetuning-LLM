WITH prepared AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        INITCAP(TRIM(device)) AS device_clean
    FROM plays
    WHERE track_id IS NOT NULL
      AND ms_played > 98052
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(ms_played) OVER (PARTITION BY device, listener_id ORDER BY ms_played DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS ms_played_avg_1_prior,
        ROUND(AVG(ms_played) OVER (PARTITION BY device, listener_id ORDER BY ms_played DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS ms_played_avg_7,
        MAX(ms_played) OVER (PARTITION BY is_skipped ORDER BY ms_played, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS ms_played_max_2,
        SUM(ms_played) OVER w_listener_id AS listener_id_sum_ms_played
    FROM prepared
    WINDOW w_listener_id AS (PARTITION BY listener_id)
),
device_stats AS (
    -- Max ms_played_avg_1_prior per device
    SELECT device, MAX(ms_played_avg_1_prior) AS max_ms_played_avg_1_prior
    FROM metrics
    GROUP BY device
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_ms_played_avg_1_prior,
        ROUND(s.ms_played_avg_1_prior - g.max_ms_played_avg_1_prior, 2) AS ms_played_avg_1_prior_minus_max
    FROM metrics s
    JOIN device_stats g ON g.device = s.device
    WHERE s.ms_played_avg_1_prior = g.max_ms_played_avg_1_prior
),
line_level AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.is_skipped,
        s.device_clean,
        s.ms_played_avg_7,
        s.ms_played_max_2,
        s.listener_id_sum_ms_played,
        s.max_ms_played_avg_1_prior,
        s.ms_played_avg_1_prior_minus_max,
        p.id AS play_id,
        p.track_id AS play_track_id,
        p.ms_played AS play_ms_played
    FROM outliers s
    JOIN plays p ON p.listener_id = s.listener_id
)
SELECT
    track_id,
    listener_id,
    COUNT(DISTINCT listener_id) AS distinct_listener_ids
FROM line_level
GROUP BY CUBE(track_id, listener_id)
ORDER BY track_id NULLS LAST, listener_id NULLS LAST
