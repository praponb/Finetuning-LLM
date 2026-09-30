WITH recent_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        SUBSTRING(TRIM(device), 1, 4) AS device_prefix,
        COALESCE(ms_played, 0) AS ms_played_filled
    FROM plays
    WHERE source = 'radio'
      AND track_id IS NOT NULL
),
screened AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM recent_plays s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.listener_id
              AND p.played_at >= TIMESTAMP '2025-01-13 12:00:00'
        )
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, amount
FROM (
    SELECT id, ms_played_filled, ms_played
    FROM screened
) src
UNPIVOT INCLUDE NULLS (
    amount FOR measure IN (ms_played_filled, ms_played)
) unp
