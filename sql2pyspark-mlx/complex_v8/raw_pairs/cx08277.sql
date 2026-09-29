WITH recent_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm,
        GREATEST(bpm, duration_sec) AS greatest_bpm_duration_sec
    FROM tracks
    WHERE artist_id IS NOT NULL
),
with_activity AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM recent_tracks s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.id
              AND p.listener_id IS NOT NULL
              AND p.ms_played BETWEEN 112786 AND 245815
        )
)
SELECT id, metric_name, value
FROM (
    SELECT id, bpm, duration_sec, greatest_bpm_duration_sec
    FROM with_activity
) src
UNPIVOT (
    value FOR metric_name IN (bpm AS m_bpm, duration_sec AS m_duration_sec, greatest_bpm_duration_sec AS greatest_bpm_duration_sec_v)
) unp
