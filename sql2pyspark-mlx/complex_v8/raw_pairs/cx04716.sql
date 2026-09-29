WITH car_monthly AS (
    -- car rows per played_at_month
    SELECT DATE_TRUNC('MONTH', played_at) AS played_at_month, COUNT(*) AS car_count, SUM(ms_played) AS car_total_ms_played
    FROM plays
    WHERE device = 'car'
    GROUP BY DATE_TRUNC('MONTH', played_at)
),
speaker_monthly AS (
    -- speaker rows per played_at_month
    SELECT DATE_TRUNC('MONTH', played_at) AS played_at_month, COUNT(*) AS speaker_count, ROUND(AVG(ms_played), 2) AS speaker_avg_ms_played
    FROM plays
    WHERE device = 'speaker'
    GROUP BY DATE_TRUNC('MONTH', played_at)
)
-- Combine both sides; keys missing on one side get zeros
SELECT
    COALESCE(a.played_at_month, b.played_at_month) AS played_at_month,
    COALESCE(a.car_count, 0) AS car_count,
    a.car_total_ms_played,
    COALESCE(b.speaker_count, 0) AS speaker_count,
    b.speaker_avg_ms_played,
    COALESCE(a.car_count, 0) - COALESCE(b.speaker_count, 0) AS car_count_minus_speaker_count
FROM car_monthly a
FULL OUTER JOIN speaker_monthly b
  ON a.played_at_month = b.played_at_month
ORDER BY played_at_month
