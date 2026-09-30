WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source,
        COALESCE(device, 'UNKNOWN') AS device_or_unknown,
        LOWER(TRIM(device)) AS device_clean
    FROM plays
    WHERE source NOT IN ('album', 'playlist')
),
without_matches AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM scoped s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.listener_id
              AND p.ms_played < 302279
        )
      AND s.track_id IS NOT NULL
),
with_parent_track AS (
    -- Join tracks
    SELECT s.*, t.duration_sec AS track_duration_sec, t.bpm AS track_bpm
    FROM without_matches s
    LEFT JOIN tracks t ON t.id = s.track_id AND ARRAY_CONTAINS(t.moods, 'sad')
),
combined_details AS (
    -- Join tracks
    SELECT s.*, t.moods AS track_moods, t.bpm AS track_bpm_2
    FROM with_parent_track s
    LEFT JOIN tracks t ON t.id = s.track_id
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, reading
FROM (
    SELECT id, track_bpm_2, track_bpm, track_duration_sec
    FROM combined_details
) src
UNPIVOT (
    reading FOR measure IN (track_bpm_2 AS m_track_bpm_2, track_bpm AS track_bpm_v, track_duration_sec AS m_track_duration_sec)
) unp
