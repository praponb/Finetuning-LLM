WITH speaker_monthly AS (
    -- speaker rows per played_at_month
    SELECT DATE_TRUNC('MONTH', played_at) AS played_at_month, COUNT(*) AS speaker_count, SUM(ms_played) AS speaker_total_ms_played
    FROM plays
    WHERE device = 'speaker'
    GROUP BY DATE_TRUNC('MONTH', played_at)
),
phone_monthly AS (
    -- phone rows per played_at_month
    SELECT DATE_TRUNC('MONTH', played_at) AS played_at_month, COUNT(*) AS phone_count, ROUND(AVG(ms_played), 2) AS phone_avg_ms_played
    FROM plays
    WHERE device = 'phone'
    GROUP BY DATE_TRUNC('MONTH', played_at)
)
SELECT
    COALESCE(a.played_at_month, b.played_at_month) AS played_at_month,
    COALESCE(a.speaker_count, 0) AS speaker_count,
    a.speaker_total_ms_played,
    COALESCE(b.phone_count, 0) AS phone_count,
    b.phone_avg_ms_played,
    COALESCE(a.speaker_count, 0) - COALESCE(b.phone_count, 0) AS net_count
FROM speaker_monthly a
FULL OUTER JOIN phone_monthly b
  ON a.played_at_month = b.played_at_month
ORDER BY played_at_month
