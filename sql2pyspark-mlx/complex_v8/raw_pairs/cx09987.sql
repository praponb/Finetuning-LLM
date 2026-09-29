WITH cleaned AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        ABS(ms_played - 130888) AS ms_played_distance
    FROM plays
    WHERE device IN ('car', 'phone', 'desktop')
      AND listener_id IS NOT NULL
),
with_parent_track AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.device,
        s.is_skipped,
        t.title AS track_title,
        t.artist_id AS track_artist_id
    FROM cleaned s
    LEFT JOIN tracks t ON t.id = s.track_id
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY track_id ORDER BY ms_played ASC), 3) AS ms_played_cume_dist,
        MAX(ms_played) OVER (PARTITION BY track_id) AS track_id_max_ms_played,
        ROUND(AVG(ms_played) OVER (PARTITION BY track_id), 2) AS track_id_avg_ms_played,
        NTH_VALUE(ms_played, 2) OVER (PARTITION BY device, track_artist_id ORDER BY ms_played DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ms_played
    FROM with_parent_track
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT track_artist_id, is_skipped, device, track_id_max_ms_played
    FROM with_trends
) src
PIVOT (
    MAX(track_id_max_ms_played)
    FOR device IN ('speaker', 'desktop')
)
ORDER BY track_artist_id, is_skipped
