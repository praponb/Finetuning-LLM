WITH car_by_track AS (
    -- car rows per track_id
    SELECT track_id, COUNT(*) AS car_count, ROUND(AVG(ms_played), 2) AS car_avg_ms_played
    FROM plays
    WHERE device = 'car'
    GROUP BY track_id
),
phone_by_track AS (
    -- phone rows per track_id
    SELECT track_id, COUNT(*) AS phone_count, SUM(ms_played) AS phone_total_ms_played
    FROM plays
    WHERE device = 'phone'
    GROUP BY track_id
)
SELECT
    COALESCE(a.track_id, b.track_id) AS track_id,
    COALESCE(a.car_count, 0) AS car_count,
    COALESCE(a.car_avg_ms_played, 0) AS car_avg_ms_played,
    COALESCE(b.phone_count, 0) AS phone_count,
    b.phone_total_ms_played,
    COALESCE(a.car_count, 0) - COALESCE(b.phone_count, 0) AS net_count
FROM car_by_track a
FULL OUTER JOIN phone_by_track b
  ON a.track_id = b.track_id
ORDER BY track_id
