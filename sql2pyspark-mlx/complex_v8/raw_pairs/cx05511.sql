WITH phone_monthly AS (
    -- phone rows per played_at_month
    SELECT DATE_TRUNC('MONTH', played_at) AS played_at_month, COUNT(*) AS phone_count, SUM(ms_played) AS phone_total_ms_played
    FROM plays
    WHERE device = 'phone'
    GROUP BY DATE_TRUNC('MONTH', played_at)
),
car_monthly AS (
    -- car rows per played_at_month
    SELECT DATE_TRUNC('MONTH', played_at) AS played_at_month, COUNT(*) AS car_count, SUM(ms_played) AS car_total_ms_played
    FROM plays
    WHERE device = 'car'
    GROUP BY DATE_TRUNC('MONTH', played_at)
)
SELECT
    COALESCE(a.played_at_month, b.played_at_month) AS played_at_month,
    COALESCE(a.phone_count, 0) AS phone_count,
    a.phone_total_ms_played,
    COALESCE(b.car_count, 0) AS car_count,
    b.car_total_ms_played,
    COALESCE(a.phone_count, 0) - COALESCE(b.car_count, 0) AS phone_count_minus_car_count
FROM phone_monthly a
FULL OUTER JOIN car_monthly b
  ON a.played_at_month = b.played_at_month
ORDER BY played_at_month
