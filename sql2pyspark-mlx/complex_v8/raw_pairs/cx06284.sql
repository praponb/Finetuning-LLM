WITH scoped_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped
    FROM plays
    WHERE source = 'search'
      AND device = 'desktop'
)
SELECT track_id
FROM scoped_plays
INTERSECT
SELECT id
FROM artists
WHERE debut_year BETWEEN 1990 AND 2015
  AND genre = 'Classical'
ORDER BY track_id
