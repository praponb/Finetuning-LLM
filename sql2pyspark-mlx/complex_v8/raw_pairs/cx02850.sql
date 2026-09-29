WITH cleaned_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        source
    FROM plays
    WHERE played_at >= TIMESTAMP '2025-01-31 12:00:00'
),
without_matches AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM cleaned_plays s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.track_id
              AND p.listener_id >= 13
              AND p.device NOT IN ('tv', 'car')
        )
)
-- Subtotals with GROUPING SETS
SELECT
    source,
    device,
    ARRAY_SORT(COLLECT_SET(source)) AS source_list,
    COUNT(*) AS row_count
FROM without_matches
GROUP BY GROUPING SETS ((source, device), (source), (device))
ORDER BY source NULLS LAST, device NULLS LAST
