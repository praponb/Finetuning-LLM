WITH base AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        TO_DATE(played_at) AS played_at_date,
        LENGTH(TRIM(device)) AS device_len,
        played_at + INTERVAL 2 HOURS AS played_at_plus_2h
    FROM plays
    WHERE played_at >= TIMESTAMP '2025-01-31 10:00:00'
),
qualified AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM base s
    WHERE s.listener_id IN (
            SELECT p.listener_id
            FROM plays p
        )
)
SELECT *
FROM (
    SELECT device_len, is_skipped, device, ms_played
    FROM qualified
) src
PIVOT (
    SUM(ms_played)
    FOR device IN ('tv', 'car', 'desktop')
)
