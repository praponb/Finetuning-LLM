WITH speaker_by_track AS (
    -- speaker rows per track_id
    SELECT track_id, COUNT(*) AS speaker_count, SUM(ms_played) AS speaker_total_ms_played
    FROM plays
    WHERE device = 'speaker'
    GROUP BY track_id
),
desktop_by_track AS (
    -- desktop rows per track_id
    SELECT track_id, COUNT(*) AS desktop_count, ROUND(AVG(ms_played), 2) AS desktop_avg_ms_played
    FROM plays
    WHERE device = 'desktop'
    GROUP BY track_id
)
SELECT
    COALESCE(a.track_id, b.track_id) AS track_id,
    COALESCE(a.speaker_count, 0) AS speaker_count,
    COALESCE(a.speaker_total_ms_played, 0) AS speaker_total_ms_played,
    COALESCE(b.desktop_count, 0) AS desktop_count,
    b.desktop_avg_ms_played,
    COALESCE(a.speaker_count, 0) - COALESCE(b.desktop_count, 0) AS net_count
FROM speaker_by_track a
FULL OUTER JOIN desktop_by_track b
  ON a.track_id = b.track_id
ORDER BY track_id
