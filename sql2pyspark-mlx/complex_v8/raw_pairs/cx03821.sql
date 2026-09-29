WITH car_by_listener AS (
    -- car rows per listener_id
    SELECT listener_id, COUNT(*) AS car_count, ROUND(AVG(ms_played), 2) AS car_avg_ms_played
    FROM plays
    WHERE device = 'car'
    GROUP BY listener_id
),
phone_by_listener AS (
    -- phone rows per listener_id
    SELECT listener_id, COUNT(*) AS phone_count, SUM(ms_played) AS phone_total_ms_played
    FROM plays
    WHERE device = 'phone'
    GROUP BY listener_id
)
-- Combine both sides; keys missing on one side get zeros
SELECT
    COALESCE(a.listener_id, b.listener_id) AS listener_id,
    COALESCE(a.car_count, 0) AS car_count,
    a.car_avg_ms_played,
    COALESCE(b.phone_count, 0) AS phone_count,
    COALESCE(b.phone_total_ms_played, 0) AS phone_total_ms_played,
    COALESCE(a.car_count, 0) - COALESCE(b.phone_count, 0) AS car_count_minus_phone_count
FROM car_by_listener a
FULL OUTER JOIN phone_by_listener b
  ON a.listener_id = b.listener_id
ORDER BY listener_id
