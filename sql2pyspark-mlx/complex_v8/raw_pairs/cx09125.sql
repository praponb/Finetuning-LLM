WITH car_monthly AS (
    -- car rows per played_at_month
    SELECT DATE_TRUNC('MONTH', played_at) AS played_at_month, COUNT(*) AS car_count, ROUND(AVG(ms_played), 2) AS car_avg_ms_played
    FROM plays
    WHERE device = 'car'
    GROUP BY DATE_TRUNC('MONTH', played_at)
),
desktop_monthly AS (
    -- desktop rows per played_at_month
    SELECT DATE_TRUNC('MONTH', played_at) AS played_at_month, COUNT(*) AS desktop_count, ROUND(AVG(ms_played), 2) AS desktop_avg_ms_played
    FROM plays
    WHERE device = 'desktop'
    GROUP BY DATE_TRUNC('MONTH', played_at)
)
SELECT
    COALESCE(a.played_at_month, b.played_at_month) AS played_at_month,
    COALESCE(a.car_count, 0) AS car_count,
    a.car_avg_ms_played,
    COALESCE(b.desktop_count, 0) AS desktop_count,
    b.desktop_avg_ms_played,
    COALESCE(a.car_count, 0) - COALESCE(b.desktop_count, 0) AS net_count
FROM car_monthly a
FULL OUTER JOIN desktop_monthly b
  ON a.played_at_month = b.played_at_month
ORDER BY played_at_month
