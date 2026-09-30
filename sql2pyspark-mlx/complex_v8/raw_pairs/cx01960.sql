WITH scoped AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        source,
        DATE_FORMAT(played_at, 'yyyy-MM') AS played_at_label,
        COALESCE(device, 'UNKNOWN') AS device_or_unknown
    FROM plays
    WHERE listener_id <= 11
),
deduped AS (
    -- Keep the earliest row per source, device
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        source,
        played_at_label,
        device_or_unknown
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY source, device ORDER BY played_at, id) AS rn
        FROM scoped
    ) ranked
    WHERE rn = 1
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        listener_id,
        track_id,
        device,
        played_at_label,
        device_or_unknown,
        LAST_VALUE(played_at) OVER (PARTITION BY track_id ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_played_at,
        NTH_VALUE(played_at, 2) OVER (PARTITION BY track_id ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at
    FROM deduped
)
-- Subtotals with CUBE
SELECT
    listener_id,
    device,
    SUM(CASE WHEN device IN ('tv', 'phone', 'car') THEN 1 ELSE 0 END) AS n_device_in_tv_phone_car,
    COUNT(*) AS cnt
FROM windowed
GROUP BY CUBE(listener_id, device)
ORDER BY listener_id NULLS LAST, device NULLS LAST
