WITH recent AS (
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
    WHERE (played_at >= TIMESTAMP '2025-02-06 08:00:00' OR track_id <= 18)
),
tagged AS (
    -- Label and score rows
    SELECT *, SUBSTRING(TRIM(device), 1, 4) AS device_prefix
    FROM recent
    WHERE listener_id >= 14
)
-- Subtotals with GROUPING SETS
SELECT
    track_id,
    listener_id,
    COALESCE(device, 'ALL') AS device,
    COUNT(*) AS cnt,
    MIN(played_at) AS first_played_at,
    ROUND(AVG(ms_played), 2) AS avg_ms_played
FROM tagged
GROUP BY GROUPING SETS ((track_id, listener_id, device), (track_id), ())
ORDER BY track_id NULLS LAST, listener_id NULLS LAST, device NULLS LAST
