WITH phone_by_listener AS (
    -- phone rows per listener_id
    SELECT listener_id, COUNT(*) AS phone_count, ROUND(AVG(ms_played), 2) AS phone_avg_ms_played
    FROM plays
    WHERE device = 'phone'
    GROUP BY listener_id
),
car_by_listener AS (
    -- car rows per listener_id
    SELECT listener_id, COUNT(*) AS car_count, SUM(ms_played) AS car_total_ms_played
    FROM plays
    WHERE device = 'car'
    GROUP BY listener_id
)
-- Combine both sides; keys missing on one side get zeros
SELECT
    COALESCE(a.listener_id, b.listener_id) AS listener_id,
    COALESCE(a.phone_count, 0) AS phone_count,
    COALESCE(a.phone_avg_ms_played, 0) AS phone_avg_ms_played,
    COALESCE(b.car_count, 0) AS car_count,
    COALESCE(b.car_total_ms_played, 0) AS car_total_ms_played,
    COALESCE(a.phone_count, 0) - COALESCE(b.car_count, 0) AS net_count
FROM phone_by_listener a
FULL OUTER JOIN car_by_listener b
  ON a.listener_id = b.listener_id
ORDER BY listener_id
