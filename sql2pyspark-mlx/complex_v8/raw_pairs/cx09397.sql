WITH desktop_by_track AS (
    -- desktop rows per track_id
    SELECT track_id, COUNT(*) AS desktop_count, SUM(ms_played) AS desktop_total_ms_played
    FROM plays
    WHERE device = 'desktop'
    GROUP BY track_id
),
car_by_track AS (
    -- car rows per track_id
    SELECT track_id, COUNT(*) AS car_count, SUM(ms_played) AS car_total_ms_played
    FROM plays
    WHERE device = 'car'
    GROUP BY track_id
)
SELECT
    COALESCE(a.track_id, b.track_id) AS track_id,
    COALESCE(a.desktop_count, 0) AS desktop_count,
    COALESCE(a.desktop_total_ms_played, 0) AS desktop_total_ms_played,
    COALESCE(b.car_count, 0) AS car_count,
    b.car_total_ms_played,
    COALESCE(a.desktop_count, 0) - COALESCE(b.car_count, 0) AS desktop_count_minus_car_count
FROM desktop_by_track a
FULL OUTER JOIN car_by_track b
  ON a.track_id = b.track_id
ORDER BY track_id
