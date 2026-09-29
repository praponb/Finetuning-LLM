WITH filtered AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        INITCAP(TRIM(device)) AS device_clean
    FROM plays
    WHERE ms_played >= 72464
)
SELECT listener_id, played_at AS event_time
FROM plays
WHERE device <> 'phone'
UNION ALL
SELECT listener_id, played_at AS event_time
FROM plays
WHERE track_id <= 18
