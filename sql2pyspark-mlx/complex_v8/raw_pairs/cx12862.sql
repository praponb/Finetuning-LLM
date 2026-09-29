WITH base_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        is_skipped,
        source,
        COALESCE(ms_played, 0) AS ms_played_filled,
        played_at + INTERVAL 7 DAYS AS played_at_plus_7d
    FROM plays
    WHERE track_id IS NOT NULL
      AND is_skipped = TRUE
),
with_activity AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM base_plays s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.listener_id
              AND p.ms_played <= 251066
        )
),
filtered AS (
    -- Keep rows whose track matches
    SELECT s.*
    FROM with_activity s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.id = s.track_id
              AND t.duration_sec <= 392
              AND SIZE(t.moods) >= 2
        )
),
screened AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.track_id
              AND p.listener_id <= 17
        )
)
SELECT
    id,
    track_id,
    played_at,
    is_skipped,
    source,
    ms_played_filled,
    played_at_plus_7d
FROM screened
ORDER BY played_at_plus_7d
