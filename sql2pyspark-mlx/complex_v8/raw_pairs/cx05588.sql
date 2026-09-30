WITH prepared_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source
    FROM plays
    WHERE HOUR(played_at) BETWEEN 8 AND 12
      AND device = 'speaker'
)
SELECT listener_id, played_at
FROM prepared_plays
WHERE NOT is_skipped
UNION ALL
SELECT track_id, played_at
FROM plays
ORDER BY played_at DESC
