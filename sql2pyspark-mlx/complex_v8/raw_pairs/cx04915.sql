WITH speaker_by_listener AS (
    -- speaker rows per listener_id
    SELECT listener_id, COUNT(*) AS speaker_count, SUM(ms_played) AS speaker_total_ms_played
    FROM plays
    WHERE device = 'speaker'
    GROUP BY listener_id
),
phone_by_listener AS (
    -- phone rows per listener_id
    SELECT listener_id, COUNT(*) AS phone_count, ROUND(AVG(ms_played), 2) AS phone_avg_ms_played
    FROM plays
    WHERE device = 'phone'
    GROUP BY listener_id
)
SELECT
    COALESCE(a.listener_id, b.listener_id) AS listener_id,
    COALESCE(a.speaker_count, 0) AS speaker_count,
    COALESCE(a.speaker_total_ms_played, 0) AS speaker_total_ms_played,
    COALESCE(b.phone_count, 0) AS phone_count,
    COALESCE(b.phone_avg_ms_played, 0) AS phone_avg_ms_played,
    COALESCE(a.speaker_count, 0) - COALESCE(b.phone_count, 0) AS net_count
FROM speaker_by_listener a
FULL OUTER JOIN phone_by_listener b
  ON a.listener_id = b.listener_id
ORDER BY listener_id
