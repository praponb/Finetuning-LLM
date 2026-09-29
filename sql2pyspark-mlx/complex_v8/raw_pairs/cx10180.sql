WITH recent_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        source,
        ABS(ms_played - 178909) AS ms_played_distance,
        CONCAT(source, '#', CAST(ms_played AS STRING)) AS source_ref,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket
    FROM plays
    WHERE listener_id IS NOT NULL
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        GREATEST(ms_played, ms_played_bucket) AS greatest_ms_played_ms_played_bucket
    FROM recent_plays
)
-- Subtotals with GROUPING SETS
SELECT
    track_id,
    source,
    device,
    GROUPING_ID() AS grouping_level,
    SUM(greatest_ms_played_ms_played_bucket) AS total_greatest_ms_played_ms_played_bucket,
    COUNT(*) AS row_count
FROM labeled
GROUP BY GROUPING SETS ((track_id, source, device), (track_id), (source))
ORDER BY track_id NULLS LAST, source NULLS LAST, device NULLS LAST
