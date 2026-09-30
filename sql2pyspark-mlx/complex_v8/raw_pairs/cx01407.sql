WITH desktop_by_listener AS (
    -- desktop rows per listener_id
    SELECT listener_id, COUNT(*) AS desktop_count, SUM(ms_played) AS desktop_total_ms_played
    FROM plays
    WHERE device = 'desktop'
    GROUP BY listener_id
),
speaker_by_listener AS (
    -- speaker rows per listener_id
    SELECT listener_id, COUNT(*) AS speaker_count, SUM(ms_played) AS speaker_total_ms_played
    FROM plays
    WHERE device = 'speaker'
    GROUP BY listener_id
)
SELECT
    COALESCE(a.listener_id, b.listener_id) AS listener_id,
    COALESCE(a.desktop_count, 0) AS desktop_count,
    a.desktop_total_ms_played,
    COALESCE(b.speaker_count, 0) AS speaker_count,
    COALESCE(b.speaker_total_ms_played, 0) AS speaker_total_ms_played,
    COALESCE(a.desktop_count, 0) - COALESCE(b.speaker_count, 0) AS desktop_count_minus_speaker_count
FROM desktop_by_listener a
FULL OUTER JOIN speaker_by_listener b
  ON a.listener_id = b.listener_id
ORDER BY listener_id
