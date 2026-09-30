WITH prepared AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        source
    FROM plays
),
with_details_listener AS (
    -- Join listeners
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.device,
        s.source,
        l.plan AS listener_plan
    FROM prepared s
    LEFT JOIN listeners l ON l.id = s.listener_id
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        INITCAP(TRIM(source)) AS source_clean,
        CONCAT_WS(': ', source, device) AS source_device_label
    FROM with_details_listener
    WHERE track_id IS NOT NULL
)
SELECT
    id,
    track_id,
    device,
    listener_plan,
    source_clean,
    source_device_label,
    CONCAT_WS(' / ', listener_plan, source_clean) AS listener_plan_source_clean_label,
    COALESCE(listener_plan, 'UNKNOWN') AS listener_plan_or_unknown
FROM scored
ORDER BY device
