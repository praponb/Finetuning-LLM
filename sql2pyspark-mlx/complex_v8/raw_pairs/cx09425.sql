WITH phone_by_track AS (
    -- phone rows per track_id
    SELECT track_id, COUNT(*) AS phone_count, ROUND(AVG(ms_played), 2) AS phone_avg_ms_played
    FROM plays
    WHERE device = 'phone'
    GROUP BY track_id
),
speaker_by_track AS (
    -- speaker rows per track_id
    SELECT track_id, COUNT(*) AS speaker_count, ROUND(AVG(ms_played), 2) AS speaker_avg_ms_played
    FROM plays
    WHERE device = 'speaker'
    GROUP BY track_id
)
-- Combine both sides; keys missing on one side get zeros
SELECT
    COALESCE(a.track_id, b.track_id) AS track_id,
    COALESCE(a.phone_count, 0) AS phone_count,
    COALESCE(a.phone_avg_ms_played, 0) AS phone_avg_ms_played,
    COALESCE(b.speaker_count, 0) AS speaker_count,
    COALESCE(b.speaker_avg_ms_played, 0) AS speaker_avg_ms_played,
    COALESCE(a.phone_count, 0) - COALESCE(b.speaker_count, 0) AS net_count
FROM phone_by_track a
FULL OUTER JOIN speaker_by_track b
  ON a.track_id = b.track_id
ORDER BY track_id
