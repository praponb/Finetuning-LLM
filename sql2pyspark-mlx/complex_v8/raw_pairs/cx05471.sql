WITH desktop_monthly AS (
    -- desktop rows per played_at_month
    SELECT DATE_TRUNC('MONTH', played_at) AS played_at_month, COUNT(*) AS desktop_count, ROUND(AVG(ms_played), 2) AS desktop_avg_ms_played
    FROM plays
    WHERE device = 'desktop'
    GROUP BY DATE_TRUNC('MONTH', played_at)
),
phone_monthly AS (
    -- phone rows per played_at_month
    SELECT DATE_TRUNC('MONTH', played_at) AS played_at_month, COUNT(*) AS phone_count, ROUND(AVG(ms_played), 2) AS phone_avg_ms_played
    FROM plays
    WHERE device = 'phone'
    GROUP BY DATE_TRUNC('MONTH', played_at)
)
-- Combine both sides; keys missing on one side get zeros
SELECT
    COALESCE(a.played_at_month, b.played_at_month) AS played_at_month,
    COALESCE(a.desktop_count, 0) AS desktop_count,
    a.desktop_avg_ms_played,
    COALESCE(b.phone_count, 0) AS phone_count,
    b.phone_avg_ms_played,
    COALESCE(a.desktop_count, 0) - COALESCE(b.phone_count, 0) AS desktop_count_minus_phone_count
FROM desktop_monthly a
FULL OUTER JOIN phone_monthly b
  ON a.played_at_month = b.played_at_month
ORDER BY played_at_month
