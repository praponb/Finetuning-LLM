WITH valid_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        source,
        COALESCE(device, 'UNKNOWN') AS device_or_unknown
    FROM plays
    WHERE NOT is_skipped
),
device_or_unknown_stats AS (
    -- Avg ms_played per device_or_unknown
    SELECT device_or_unknown, AVG(ms_played) AS avg_ms_played
    FROM valid_plays
    GROUP BY device_or_unknown
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_ms_played,
        ROUND(s.ms_played - g.avg_ms_played, 2) AS ms_played_minus_avg
    FROM valid_plays s
    JOIN device_or_unknown_stats g ON g.device_or_unknown = s.device_or_unknown
    WHERE s.ms_played > g.avg_ms_played
),
with_activity AS (
    -- Keep rows whose listener matches
    SELECT s.*
    FROM outliers s
    WHERE EXISTS (
            SELECT 1
            FROM listeners l
            WHERE l.id = s.listener_id
              AND NOT ARRAY_CONTAINS(l.interests, 'live')
              AND l.plan <> 'free'
        )
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        RANK() OVER w_all_by_ms_played_ties AS ms_played_rank,
        SIZE(COLLECT_SET(device) OVER (PARTITION BY listener_id, source)) AS distinct_device_in_listener_id_source,
        COUNT(*) OVER w_listener_id_source AS rows_in_listener_id_source,
        ROUND(CUME_DIST() OVER (ORDER BY avg_ms_played), 3) AS avg_ms_played_cume_dist,
        ms_played - LAG(ms_played) OVER (ORDER BY avg_ms_played, id) AS ms_played_change,
        SIZE(COLLECT_SET(device_or_unknown) OVER (PARTITION BY listener_id, source)) AS distinct_device_or_unknown_in_listener_id_source
    FROM with_activity
    WINDOW w_all_by_ms_played_ties AS (ORDER BY ms_played), w_listener_id_source AS (PARTITION BY listener_id, source)
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        DAYOFWEEK(played_at) AS played_at_dayofweek,
        COALESCE(device_or_unknown, 'UNKNOWN') AS device_or_unknown_or_unknown
    FROM windowed
    WHERE ms_played >= 112786
),
profiled AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM plays p WHERE p.track_id = s.track_id) AS play_count
    FROM derived s
),
with_parent AS (
    -- Attach listener attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.device,
        s.device_or_unknown,
        s.avg_ms_played,
        s.ms_played_minus_avg,
        s.ms_played_rank,
        s.rows_in_listener_id_source,
        s.ms_played_change,
        s.distinct_device_or_unknown_in_listener_id_source,
        s.played_at_dayofweek,
        s.device_or_unknown_or_unknown,
        l.country AS listener_country,
        t.title AS track_title,
        t.bpm AS track_bpm
    FROM profiled s
    JOIN listeners l ON l.id = s.listener_id
    LEFT JOIN tracks t ON t.id = s.track_id
    WHERE l.country NOT IN ('KR', 'TH')
)
SELECT
    id,
    listener_id,
    track_id,
    played_at,
    avg_ms_played,
    ms_played_minus_avg,
    ms_played_rank,
    distinct_device_or_unknown_in_listener_id_source,
    device_or_unknown_or_unknown,
    track_title,
    ABS(track_bpm - 100) AS track_bpm_distance
FROM with_parent
WHERE ms_played_rank >= 5
ORDER BY avg_ms_played NULLS LAST
