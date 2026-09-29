WITH speaker_by_listener AS (
    -- speaker rows per listener_id
    SELECT listener_id, COUNT(*) AS speaker_count, ROUND(AVG(ms_played), 2) AS speaker_avg_ms_played
    FROM plays
    WHERE device = 'speaker'
    GROUP BY listener_id
),
car_by_listener AS (
    -- car rows per listener_id
    SELECT listener_id, COUNT(*) AS car_count, ROUND(AVG(ms_played), 2) AS car_avg_ms_played
    FROM plays
    WHERE device = 'car'
    GROUP BY listener_id
)
SELECT
    COALESCE(a.listener_id, b.listener_id) AS listener_id,
    COALESCE(a.speaker_count, 0) AS speaker_count,
    a.speaker_avg_ms_played,
    COALESCE(b.car_count, 0) AS car_count,
    COALESCE(b.car_avg_ms_played, 0) AS car_avg_ms_played,
    COALESCE(a.speaker_count, 0) - COALESCE(b.car_count, 0) AS net_count
FROM speaker_by_listener a
FULL OUTER JOIN car_by_listener b
  ON a.listener_id = b.listener_id
ORDER BY listener_id
