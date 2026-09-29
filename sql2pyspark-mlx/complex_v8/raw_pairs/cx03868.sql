WITH prepared_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        CONCAT_WS(' / ', source, device) AS source_device_label
    FROM plays
    WHERE device = 'speaker'
),
with_prev AS (
    -- Previous played_at per track_id
    SELECT *, LAG(played_at) OVER (PARTITION BY track_id ORDER BY played_at, id) AS prev_played_at
    FROM prepared_plays
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_played_at IS NULL OR played_at > prev_played_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY track_id ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
)
-- Final projection
SELECT
    id,
    listener_id,
    played_at,
    starts_session,
    burst_id,
    CASE WHEN device IN ('tv', 'phone') THEN 'group_a' WHEN device = 'car' THEN 'group_b' ELSE 'other' END AS device_group
FROM sessionized
WHERE prev_played_at < TIMESTAMP '2025-02-12 10:00:00'
ORDER BY listener_id DESC, burst_id, id
LIMIT 10
