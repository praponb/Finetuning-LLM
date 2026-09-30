WITH phone_by_listener AS (
    -- phone rows per listener_id
    SELECT listener_id, COUNT(*) AS phone_count, ROUND(AVG(ms_played), 2) AS phone_avg_ms_played
    FROM plays
    WHERE device = 'phone'
    GROUP BY listener_id
),
desktop_by_listener AS (
    -- desktop rows per listener_id
    SELECT listener_id, COUNT(*) AS desktop_count, SUM(ms_played) AS desktop_total_ms_played
    FROM plays
    WHERE device = 'desktop'
    GROUP BY listener_id
)
SELECT
    COALESCE(a.listener_id, b.listener_id) AS listener_id,
    COALESCE(a.phone_count, 0) AS phone_count,
    a.phone_avg_ms_played,
    COALESCE(b.desktop_count, 0) AS desktop_count,
    b.desktop_total_ms_played,
    COALESCE(a.phone_count, 0) - COALESCE(b.desktop_count, 0) AS net_count
FROM phone_by_listener a
FULL OUTER JOIN desktop_by_listener b
  ON a.listener_id = b.listener_id
ORDER BY listener_id
