WITH phone_monthly AS (
    -- phone rows per played_at_month
    SELECT DATE_TRUNC('MONTH', played_at) AS played_at_month, COUNT(*) AS phone_count, SUM(ms_played) AS phone_total_ms_played
    FROM plays
    WHERE device = 'phone'
    GROUP BY DATE_TRUNC('MONTH', played_at)
),
desktop_monthly AS (
    -- desktop rows per played_at_month
    SELECT DATE_TRUNC('MONTH', played_at) AS played_at_month, COUNT(*) AS desktop_count, SUM(ms_played) AS desktop_total_ms_played
    FROM plays
    WHERE device = 'desktop'
    GROUP BY DATE_TRUNC('MONTH', played_at)
)
-- Combine both sides; keys missing on one side get zeros
SELECT
    COALESCE(a.played_at_month, b.played_at_month) AS played_at_month,
    COALESCE(a.phone_count, 0) AS phone_count,
    COALESCE(a.phone_total_ms_played, 0) AS phone_total_ms_played,
    COALESCE(b.desktop_count, 0) AS desktop_count,
    COALESCE(b.desktop_total_ms_played, 0) AS desktop_total_ms_played,
    COALESCE(a.phone_count, 0) - COALESCE(b.desktop_count, 0) AS net_count
FROM phone_monthly a
FULL OUTER JOIN desktop_monthly b
  ON a.played_at_month = b.played_at_month
ORDER BY played_at_month
