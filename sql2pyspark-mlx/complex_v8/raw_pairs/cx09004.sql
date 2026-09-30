WITH scoped_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        CONCAT_WS(' / ', device, source) AS device_source_label
    FROM plays
    WHERE played_at < TIMESTAMP '2025-02-24 11:00:00'
      AND source = 'search'
)
SELECT track_id AS qty
FROM scoped_plays
WHERE device = 'speaker'
EXCEPT
SELECT ms_played
FROM plays
WHERE TO_DATE(played_at) < DATE '2025-02-12'
  AND device = 'tv'
ORDER BY qty
