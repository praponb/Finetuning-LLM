WITH car_by_listener AS (
    -- car rows per listener_id
    SELECT listener_id, COUNT(*) AS car_count, SUM(ms_played) AS car_total_ms_played
    FROM plays
    WHERE device = 'car'
    GROUP BY listener_id
),
speaker_by_listener AS (
    -- speaker rows per listener_id
    SELECT listener_id, COUNT(*) AS speaker_count, ROUND(AVG(ms_played), 2) AS speaker_avg_ms_played
    FROM plays
    WHERE device = 'speaker'
    GROUP BY listener_id
)
SELECT
    COALESCE(a.listener_id, b.listener_id) AS listener_id,
    COALESCE(a.car_count, 0) AS car_count,
    a.car_total_ms_played,
    COALESCE(b.speaker_count, 0) AS speaker_count,
    COALESCE(b.speaker_avg_ms_played, 0) AS speaker_avg_ms_played,
    COALESCE(a.car_count, 0) - COALESCE(b.speaker_count, 0) AS net_count
FROM car_by_listener a
FULL OUTER JOIN speaker_by_listener b
  ON a.listener_id = b.listener_id
ORDER BY listener_id
