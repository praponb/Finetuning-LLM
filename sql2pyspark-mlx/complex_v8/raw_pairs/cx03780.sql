WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source
    FROM plays
    WHERE ms_played >= 157900
)
SELECT track_id AS key_id, source AS category
FROM filtered
EXCEPT ALL
SELECT id, country
FROM artists
WHERE genre IS NOT NULL
  AND debut_year < 2015
