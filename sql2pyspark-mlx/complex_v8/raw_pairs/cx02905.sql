WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        source,
        DATEDIFF(DATE '2025-06-30', played_at) AS days_since_played_at
    FROM plays
    WHERE listener_id IS NOT NULL
      AND device IN ('phone', 'speaker')
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        ABS(days_since_played_at - 350) AS days_since_played_at_distance,
        SUBSTRING(TRIM(source), 1, 3) AS source_prefix
    FROM cleaned
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(source, 'ALL') AS source,
    listener_id,
    SUM(days_since_played_at) AS total_days_since_played_at,
    COUNT(DISTINCT id) AS distinct_ids
FROM derived
GROUP BY GROUPING SETS ((source, listener_id), (source), (listener_id))
ORDER BY source NULLS LAST, listener_id NULLS LAST
