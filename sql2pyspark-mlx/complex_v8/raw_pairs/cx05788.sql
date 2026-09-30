WITH desktop_monthly AS (
    -- desktop rows per played_at_month
    SELECT DATE_TRUNC('MONTH', played_at) AS played_at_month, COUNT(*) AS desktop_count, SUM(ms_played) AS desktop_total_ms_played
    FROM plays
    WHERE device = 'desktop'
    GROUP BY DATE_TRUNC('MONTH', played_at)
),
car_monthly AS (
    -- car rows per played_at_month
    SELECT DATE_TRUNC('MONTH', played_at) AS played_at_month, COUNT(*) AS car_count, ROUND(AVG(ms_played), 2) AS car_avg_ms_played
    FROM plays
    WHERE device = 'car'
    GROUP BY DATE_TRUNC('MONTH', played_at)
)
-- Combine both sides; keys missing on one side get zeros
SELECT
    COALESCE(a.played_at_month, b.played_at_month) AS played_at_month,
    COALESCE(a.desktop_count, 0) AS desktop_count,
    a.desktop_total_ms_played,
    COALESCE(b.car_count, 0) AS car_count,
    COALESCE(b.car_avg_ms_played, 0) AS car_avg_ms_played,
    COALESCE(a.desktop_count, 0) - COALESCE(b.car_count, 0) AS desktop_count_minus_car_count
FROM desktop_monthly a
FULL OUTER JOIN car_monthly b
  ON a.played_at_month = b.played_at_month
ORDER BY played_at_month
