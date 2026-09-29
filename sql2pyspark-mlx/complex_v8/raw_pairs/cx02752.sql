WITH car_by_track AS (
    -- car rows per track_id
    SELECT track_id, COUNT(*) AS car_count, SUM(ms_played) AS car_total_ms_played
    FROM plays
    WHERE device = 'car'
    GROUP BY track_id
),
speaker_by_track AS (
    -- speaker rows per track_id
    SELECT track_id, COUNT(*) AS speaker_count, SUM(ms_played) AS speaker_total_ms_played
    FROM plays
    WHERE device = 'speaker'
    GROUP BY track_id
)
SELECT
    COALESCE(a.track_id, b.track_id) AS track_id,
    COALESCE(a.car_count, 0) AS car_count,
    COALESCE(a.car_total_ms_played, 0) AS car_total_ms_played,
    COALESCE(b.speaker_count, 0) AS speaker_count,
    COALESCE(b.speaker_total_ms_played, 0) AS speaker_total_ms_played,
    COALESCE(a.car_count, 0) - COALESCE(b.speaker_count, 0) AS car_count_minus_speaker_count
FROM car_by_track a
FULL OUTER JOIN speaker_by_track b
  ON a.track_id = b.track_id
ORDER BY track_id
