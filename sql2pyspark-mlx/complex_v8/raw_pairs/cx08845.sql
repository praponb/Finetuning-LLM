WITH desktop_by_track AS (
    -- desktop rows per track_id
    SELECT track_id, COUNT(*) AS desktop_count, SUM(ms_played) AS desktop_total_ms_played
    FROM plays
    WHERE device = 'desktop'
    GROUP BY track_id
),
speaker_by_track AS (
    -- speaker rows per track_id
    SELECT track_id, COUNT(*) AS speaker_count, SUM(ms_played) AS speaker_total_ms_played
    FROM plays
    WHERE device = 'speaker'
    GROUP BY track_id
)
-- Combine both sides; keys missing on one side get zeros
SELECT
    COALESCE(a.track_id, b.track_id) AS track_id,
    COALESCE(a.desktop_count, 0) AS desktop_count,
    a.desktop_total_ms_played,
    COALESCE(b.speaker_count, 0) AS speaker_count,
    COALESCE(b.speaker_total_ms_played, 0) AS speaker_total_ms_played,
    COALESCE(a.desktop_count, 0) - COALESCE(b.speaker_count, 0) AS net_count
FROM desktop_by_track a
FULL OUTER JOIN speaker_by_track b
  ON a.track_id = b.track_id
ORDER BY track_id
