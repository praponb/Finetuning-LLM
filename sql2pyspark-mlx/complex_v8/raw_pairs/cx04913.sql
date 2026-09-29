WITH scoped AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source,
        LOWER(TRIM(source)) AS source_clean
    FROM plays
),
with_prev AS (
    -- Previous played_at per device
    SELECT *, LAG(played_at) OVER (PARTITION BY device ORDER BY played_at, id) AS prev_played_at
    FROM scoped
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_played_at IS NULL OR (UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(prev_played_at)) / 60 > 120 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY device ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(prev_played_at) - UNIX_TIMESTAMP(played_at)) / 3600, 1) AS hours_played_at_to_prev_played_at,
        COALESCE(source, 'UNKNOWN') AS source_or_unknown
    FROM sessionized
    WHERE is_skipped = FALSE
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(source_or_unknown, 'ALL') AS source_or_unknown,
    starts_session,
    COALESCE(device, 'ALL') AS device,
    ROUND(STDDEV(starts_session), 2) AS stddev_starts_session,
    ROUND(SUM(starts_session) / NULLIF(SUM(session_no), 0), 3) AS starts_session_to_session_no_ratio
FROM derived
GROUP BY GROUPING SETS ((source_or_unknown, starts_session, device), (source_or_unknown), (starts_session), ())
ORDER BY source_or_unknown NULLS LAST, starts_session NULLS LAST, device NULLS LAST
