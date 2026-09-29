WITH prepared_plays AS (
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
        SUBSTRING(TRIM(device), 1, 4) AS device_prefix
    FROM plays
),
with_prev AS (
    -- Previous played_at per source
    SELECT *, LAG(played_at) OVER (PARTITION BY source ORDER BY played_at, id) AS prev_played_at
    FROM prepared_plays
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_played_at IS NULL OR (UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(prev_played_at)) / 60 > 30 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY source ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        is_skipped,
        is_new_session,
        burst_id,
        LENGTH(TRIM(device)) AS device_len,
        LOWER(REGEXP_REPLACE(TRIM(device_prefix), ' ', '_')) AS device_prefix_slug
    FROM sessionized
)
SELECT
    id,
    listener_id,
    track_id,
    played_at,
    is_skipped,
    is_new_session,
    burst_id,
    device_len,
    device_prefix_slug
FROM labeled
ORDER BY device_prefix_slug, device_len
