WITH prepared_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        device,
        is_skipped,
        source,
        DATEDIFF(DATE '2025-06-30', played_at) AS days_since_played_at
    FROM plays
    WHERE played_at >= TIMESTAMP '2025-01-31 12:00:00'
)
SELECT
    id,
    listener_id,
    track_id,
    is_skipped,
    ABS(days_since_played_at - 490) AS days_since_played_at_distance,
    CONCAT(source, '#', CAST(days_since_played_at AS STRING)) AS source_ref
FROM prepared_plays
WHERE days_since_played_at >= 70
ORDER BY track_id DESC NULLS FIRST
