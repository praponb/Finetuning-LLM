WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        SUBSTRING(TRIM(source), 1, 4) AS source_prefix
    FROM plays
    WHERE MONTH(played_at) IN (2, 3)
),
profiled AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM plays p WHERE p.track_id = s.track_id) AS play_count,
        (SELECT MAX(p.played_at) FROM plays p WHERE p.track_id = s.track_id) AS last_played_at,
        (SELECT SUM(p.ms_played) FROM plays p WHERE p.track_id = s.track_id) AS total_play_ms_played
    FROM prepared s
),
listener_id_stats AS (
    -- Avg total_play_ms_played per listener_id
    SELECT listener_id, AVG(total_play_ms_played) AS avg_total_play_ms_played
    FROM profiled
    GROUP BY listener_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_total_play_ms_played,
        ROUND(s.total_play_ms_played - g.avg_total_play_ms_played, 2) AS total_play_ms_played_minus_avg
    FROM profiled s
    JOIN listener_id_stats g ON g.listener_id = s.listener_id
    WHERE s.total_play_ms_played > g.avg_total_play_ms_played
)
-- Final projection
SELECT
    id,
    listener_id,
    track_id,
    played_at,
    ms_played,
    is_skipped,
    source,
    source_prefix,
    play_count,
    last_played_at,
    total_play_ms_played_minus_avg,
    LENGTH(TRIM(source_prefix)) AS source_prefix_len,
    CASE WHEN play_count >= 6 THEN 'HIGH' WHEN play_count >= 2 THEN 'MEDIUM' ELSE 'LOW' END AS play_count_tier
FROM compared
ORDER BY source_prefix DESC NULLS FIRST
