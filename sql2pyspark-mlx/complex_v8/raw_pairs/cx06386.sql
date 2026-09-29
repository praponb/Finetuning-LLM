WITH phone_by_track AS (
    -- phone rows per track_id
    SELECT track_id, COUNT(*) AS phone_count, SUM(ms_played) AS phone_total_ms_played
    FROM plays
    WHERE device = 'phone'
    GROUP BY track_id
),
car_by_track AS (
    -- car rows per track_id
    SELECT track_id, COUNT(*) AS car_count, SUM(ms_played) AS car_total_ms_played
    FROM plays
    WHERE device = 'car'
    GROUP BY track_id
)
-- Combine both sides; keys missing on one side get zeros
SELECT
    COALESCE(a.track_id, b.track_id) AS track_id,
    COALESCE(a.phone_count, 0) AS phone_count,
    a.phone_total_ms_played,
    COALESCE(b.car_count, 0) AS car_count,
    COALESCE(b.car_total_ms_played, 0) AS car_total_ms_played,
    COALESCE(a.phone_count, 0) - COALESCE(b.car_count, 0) AS phone_count_minus_car_count
FROM phone_by_track a
FULL OUTER JOIN car_by_track b
  ON a.track_id = b.track_id
ORDER BY track_id
