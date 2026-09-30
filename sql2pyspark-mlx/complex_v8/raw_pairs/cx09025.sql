WITH desktop_monthly AS (
    -- desktop rows per played_at_month
    SELECT DATE_TRUNC('MONTH', played_at) AS played_at_month, COUNT(*) AS desktop_count, SUM(ms_played) AS desktop_total_ms_played
    FROM plays
    WHERE device = 'desktop'
    GROUP BY DATE_TRUNC('MONTH', played_at)
),
speaker_monthly AS (
    -- speaker rows per played_at_month
    SELECT DATE_TRUNC('MONTH', played_at) AS played_at_month, COUNT(*) AS speaker_count, SUM(ms_played) AS speaker_total_ms_played
    FROM plays
    WHERE device = 'speaker'
    GROUP BY DATE_TRUNC('MONTH', played_at)
)
-- Combine both sides; keys missing on one side get zeros
SELECT
    COALESCE(a.played_at_month, b.played_at_month) AS played_at_month,
    COALESCE(a.desktop_count, 0) AS desktop_count,
    COALESCE(a.desktop_total_ms_played, 0) AS desktop_total_ms_played,
    COALESCE(b.speaker_count, 0) AS speaker_count,
    b.speaker_total_ms_played,
    COALESCE(a.desktop_count, 0) - COALESCE(b.speaker_count, 0) AS desktop_count_minus_speaker_count
FROM desktop_monthly a
FULL OUTER JOIN speaker_monthly b
  ON a.played_at_month = b.played_at_month
ORDER BY played_at_month
