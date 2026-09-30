WITH cleaned_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source,
        LENGTH(TRIM(device)) AS device_len
    FROM plays
    WHERE source = 'radio'
),
with_prev AS (
    -- Previous played_at per source
    SELECT *, LAG(played_at) OVER (PARTITION BY source ORDER BY played_at, id) AS prev_played_at
    FROM cleaned_plays
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_played_at IS NULL OR played_at > prev_played_at + INTERVAL 2 HOURS THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY source ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        source,
        session_id,
        MIN(played_at) AS session_start,
        MAX(played_at) AS session_end,
        COUNT(*) AS event_count,
        ROUND((UNIX_TIMESTAMP(MAX(played_at)) - UNIX_TIMESTAMP(MIN(played_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY source, session_id
),
labeled AS (
    -- Label and score rows
    SELECT *, INITCAP(TRIM(source)) AS source_clean
    FROM per_session
    WHERE source <> 'album'
),
annotated AS (
    -- Add calculated columns
    SELECT
        source,
        session_id,
        session_start,
        session_end,
        event_count,
        source_clean,
        CASE WHEN event_count >= 4 THEN 'HIGH' WHEN event_count >= 2 THEN 'MEDIUM' ELSE 'LOW' END AS event_count_tier,
        COALESCE(session_id, 0) AS session_id_filled
    FROM labeled
    WHERE session_start >= TIMESTAMP '2025-01-17 09:00:00'
)
SELECT
    source,
    session_end,
    source_clean,
    event_count_tier,
    session_id_filled
FROM annotated
ORDER BY event_count_tier DESC NULLS FIRST
