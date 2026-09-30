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
        HOUR(played_at) AS played_at_hour,
        COALESCE(device, 'UNKNOWN') AS device_or_unknown,
        ABS(ms_played - 130888) AS ms_played_distance
    FROM plays
    WHERE ms_played < 236512
),
with_parent AS (
    -- Join tracks
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.is_skipped,
        s.source,
        s.played_at_hour,
        s.device_or_unknown,
        t.artist_id AS track_artist_id
    FROM recent s
    JOIN tracks t ON t.id = s.track_id AND t.bpm > 82
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        source,
        device_or_unknown,
        ROUND(played_at_hour / NULLIF(SUM(played_at_hour) OVER (PARTITION BY device_or_unknown, track_id), 0), 4) AS played_at_hour_share_of_device_or_unknown_track_,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY device_or_unknown, track_id), 2) AS ms_played_vs_device_or_unknown_track_id_avg
    FROM with_parent
)
SELECT
    id,
    played_at,
    ms_played,
    source,
    device_or_unknown,
    played_at_hour_share_of_device_or_unknown_track_,
    ms_played_vs_device_or_unknown_track_id_avg,
    TO_DATE(played_at) AS played_at_date,
    CASE WHEN ms_played >= 287769 THEN 'HIGH' WHEN ms_played >= 73625 THEN 'MEDIUM' ELSE 'LOW' END AS ms_played_tier
FROM with_trends
ORDER BY id, ms_played
LIMIT 10
