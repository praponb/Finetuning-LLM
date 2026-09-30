WITH car_by_track AS (
    -- car rows per track_id
    SELECT track_id, COUNT(*) AS car_count, ROUND(AVG(ms_played), 2) AS car_avg_ms_played
    FROM plays
    WHERE device = 'car'
    GROUP BY track_id
),
desktop_by_track AS (
    -- desktop rows per track_id
    SELECT track_id, COUNT(*) AS desktop_count, SUM(ms_played) AS desktop_total_ms_played
    FROM plays
    WHERE device = 'desktop'
    GROUP BY track_id
)
-- Combine both sides; keys missing on one side get zeros
SELECT
    COALESCE(a.track_id, b.track_id) AS track_id,
    COALESCE(a.car_count, 0) AS car_count,
    COALESCE(a.car_avg_ms_played, 0) AS car_avg_ms_played,
    COALESCE(b.desktop_count, 0) AS desktop_count,
    b.desktop_total_ms_played,
    COALESCE(a.car_count, 0) - COALESCE(b.desktop_count, 0) AS car_count_minus_desktop_count
FROM car_by_track a
FULL OUTER JOIN desktop_by_track b
  ON a.track_id = b.track_id
ORDER BY track_id
