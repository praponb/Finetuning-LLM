WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        TO_DATE(played_at) AS played_at_date,
        DATE_FORMAT(played_at, 'yyyy-MM-dd') AS played_at_label
    FROM plays
),
screened AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.track_id
              AND p.listener_id >= 8
              AND HOUR(p.played_at) BETWEEN 10 AND 12
        )
)
-- Subtotals with GROUPING SETS
SELECT
    device,
    source,
    ROUND(AVG(ms_played), 2) AS avg_ms_played,
    ROUND(STDDEV(ms_played), 2) AS stddev_ms_played,
    COUNT(DISTINCT id) AS distinct_ids
FROM screened
GROUP BY GROUPING SETS ((device, source), (device), (source), ())
