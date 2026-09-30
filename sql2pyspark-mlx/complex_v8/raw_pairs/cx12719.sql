WITH speaker_monthly AS (
    -- speaker rows per played_at_month
    SELECT DATE_TRUNC('MONTH', played_at) AS played_at_month, COUNT(*) AS speaker_count, ROUND(AVG(ms_played), 2) AS speaker_avg_ms_played
    FROM plays
    WHERE device = 'speaker'
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
    COALESCE(a.speaker_count, 0) AS speaker_count,
    a.speaker_avg_ms_played,
    COALESCE(b.car_count, 0) AS car_count,
    COALESCE(b.car_total_ms_played, 0) AS car_total_ms_played,
    COALESCE(a.speaker_count, 0) - COALESCE(b.car_count, 0) AS speaker_count_minus_car_count
FROM speaker_monthly a
FULL OUTER JOIN car_monthly b
  ON a.played_at_month = b.played_at_month
ORDER BY played_at_month
