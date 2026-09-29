WITH base AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source,
        COALESCE(source, 'UNKNOWN') AS source_or_unknown,
        DATEDIFF(DATE '2025-12-31', played_at) AS days_since_played_at
    FROM plays
    WHERE listener_id <= 6
      AND ms_played < 223262
),
annotated AS (
    -- Derive helper columns
    SELECT *, COALESCE(device, 'UNKNOWN') AS device_or_unknown
    FROM base
    WHERE days_since_played_at < 490
),
combined_details_listener AS (
    -- Join listeners
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.device,
        s.days_since_played_at,
        l.signup_date AS listener_signup_date
    FROM annotated s
    JOIN listeners l ON l.id = s.listener_id
    WHERE l.signup_date >= DATE '2024-09-19'
)
SELECT
    listener_id,
    track_id,
    COALESCE(device, 'ALL') AS device,
    COUNT(DISTINCT device) AS distinct_devices
FROM combined_details_listener
GROUP BY CUBE(listener_id, track_id, device)
ORDER BY listener_id NULLS LAST, track_id NULLS LAST, device NULLS LAST
