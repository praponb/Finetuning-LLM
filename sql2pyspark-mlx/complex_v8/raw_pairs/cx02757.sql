WITH valid_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        SUBSTRING(TRIM(device), 1, 2) AS device_prefix,
        COALESCE(source, 'UNKNOWN') AS source_or_unknown,
        DATEDIFF(DATE '2025-06-30', played_at) AS days_since_played_at
    FROM plays
    WHERE track_id >= 22
      AND device IN ('car', 'speaker', 'phone')
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        LEAST(ms_played, days_since_played_at) AS least_ms_played_days_since_played_at
    FROM valid_plays
)
-- Subtotals with ROLLUP
SELECT
    device,
    source_or_unknown,
    MIN(ms_played) AS min_ms_played
FROM scored
GROUP BY ROLLUP(device, source_or_unknown)
ORDER BY device NULLS LAST, source_or_unknown NULLS LAST
