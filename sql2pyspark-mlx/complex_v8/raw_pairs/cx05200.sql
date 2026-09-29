WITH valid_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        COALESCE(ms_played, 0) AS ms_played_filled
    FROM plays
    WHERE source IN ('search', 'album')
      AND is_skipped = TRUE
),
filtered AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM valid_plays s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.track_id
              AND p.device <> 'tv'
              AND p.listener_id >= 18
        )
),
without_matches AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.listener_id
        )
)
SELECT id, measure, amount
FROM (
    SELECT id, ms_played, ms_played_filled
    FROM without_matches
) src
UNPIVOT INCLUDE NULLS (
    amount FOR measure IN (ms_played, ms_played_filled)
) unp
