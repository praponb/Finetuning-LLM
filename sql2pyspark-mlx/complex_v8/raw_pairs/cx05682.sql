WITH desktop_by_track AS (
    -- desktop rows per track_id
    SELECT track_id, COUNT(*) AS desktop_count, SUM(ms_played) AS desktop_total_ms_played
    FROM plays
    WHERE device = 'desktop'
    GROUP BY track_id
),
phone_by_track AS (
    -- phone rows per track_id
    SELECT track_id, COUNT(*) AS phone_count, SUM(ms_played) AS phone_total_ms_played
    FROM plays
    WHERE device = 'phone'
    GROUP BY track_id
)
-- Combine both sides; keys missing on one side get zeros
SELECT
    COALESCE(a.track_id, b.track_id) AS track_id,
    COALESCE(a.desktop_count, 0) AS desktop_count,
    a.desktop_total_ms_played,
    COALESCE(b.phone_count, 0) AS phone_count,
    COALESCE(b.phone_total_ms_played, 0) AS phone_total_ms_played,
    COALESCE(a.desktop_count, 0) - COALESCE(b.phone_count, 0) AS net_count
FROM desktop_by_track a
FULL OUTER JOIN phone_by_track b
  ON a.track_id = b.track_id
ORDER BY track_id
