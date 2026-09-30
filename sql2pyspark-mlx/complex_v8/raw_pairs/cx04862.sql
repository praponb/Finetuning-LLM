WITH speaker_by_listener AS (
    -- speaker rows per listener_id
    SELECT listener_id, COUNT(*) AS speaker_count, SUM(ms_played) AS speaker_total_ms_played
    FROM plays
    WHERE device = 'speaker'
    GROUP BY listener_id
),
phone_by_listener AS (
    -- phone rows per listener_id
    SELECT listener_id, COUNT(*) AS phone_count, SUM(ms_played) AS phone_total_ms_played
    FROM plays
    WHERE device = 'phone'
    GROUP BY listener_id
)
-- Combine both sides; keys missing on one side get zeros
SELECT
    COALESCE(a.listener_id, b.listener_id) AS listener_id,
    COALESCE(a.speaker_count, 0) AS speaker_count,
    a.speaker_total_ms_played,
    COALESCE(b.phone_count, 0) AS phone_count,
    b.phone_total_ms_played,
    COALESCE(a.speaker_count, 0) - COALESCE(b.phone_count, 0) AS speaker_count_minus_phone_count
FROM speaker_by_listener a
FULL OUTER JOIN phone_by_listener b
  ON a.listener_id = b.listener_id
ORDER BY listener_id
