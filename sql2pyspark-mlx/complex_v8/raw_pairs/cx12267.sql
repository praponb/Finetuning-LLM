WITH valid_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        is_skipped,
        source,
        DATEDIFF(DATE '2025-06-30', played_at) AS days_since_played_at
    FROM plays
    WHERE source IS NOT NULL
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM valid_plays
    WHERE ms_played > (SELECT AVG(ms_played) FROM valid_plays)
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric_name, reading
FROM (
    SELECT id, days_since_played_at, ms_played
    FROM qualified
) src
UNPIVOT INCLUDE NULLS (
    reading FOR metric_name IN (days_since_played_at, ms_played)
) unp
