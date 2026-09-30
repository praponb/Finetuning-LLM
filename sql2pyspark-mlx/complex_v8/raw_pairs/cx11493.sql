WITH car_by_listener AS (
    -- car rows per listener_id
    SELECT listener_id, COUNT(*) AS car_count, SUM(ms_played) AS car_total_ms_played
    FROM plays
    WHERE device = 'car'
    GROUP BY listener_id
),
desktop_by_listener AS (
    -- desktop rows per listener_id
    SELECT listener_id, COUNT(*) AS desktop_count, ROUND(AVG(ms_played), 2) AS desktop_avg_ms_played
    FROM plays
    WHERE device = 'desktop'
    GROUP BY listener_id
)
SELECT
    COALESCE(a.listener_id, b.listener_id) AS listener_id,
    COALESCE(a.car_count, 0) AS car_count,
    a.car_total_ms_played,
    COALESCE(b.desktop_count, 0) AS desktop_count,
    COALESCE(b.desktop_avg_ms_played, 0) AS desktop_avg_ms_played,
    COALESCE(a.car_count, 0) - COALESCE(b.desktop_count, 0) AS net_count
FROM car_by_listener a
FULL OUTER JOIN desktop_by_listener b
  ON a.listener_id = b.listener_id
ORDER BY listener_id
