WITH car_monthly AS (
    -- car rows per played_at_month
    SELECT DATE_TRUNC('MONTH', played_at) AS played_at_month, COUNT(*) AS car_count, SUM(ms_played) AS car_total_ms_played
    FROM plays
    WHERE device = 'car'
    GROUP BY DATE_TRUNC('MONTH', played_at)
),
phone_monthly AS (
    -- phone rows per played_at_month
    SELECT DATE_TRUNC('MONTH', played_at) AS played_at_month, COUNT(*) AS phone_count, SUM(ms_played) AS phone_total_ms_played
    FROM plays
    WHERE device = 'phone'
    GROUP BY DATE_TRUNC('MONTH', played_at)
)
-- Combine both sides; keys missing on one side get zeros
SELECT
    COALESCE(a.played_at_month, b.played_at_month) AS played_at_month,
    COALESCE(a.car_count, 0) AS car_count,
    a.car_total_ms_played,
    COALESCE(b.phone_count, 0) AS phone_count,
    b.phone_total_ms_played,
    COALESCE(a.car_count, 0) - COALESCE(b.phone_count, 0) AS car_count_minus_phone_count
FROM car_monthly a
FULL OUTER JOIN phone_monthly b
  ON a.played_at_month = b.played_at_month
ORDER BY played_at_month
