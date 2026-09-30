WITH valid_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source
    FROM plays
),
with_prev AS (
    -- Previous played_at per source
    SELECT *, LAG(played_at) OVER (PARTITION BY source ORDER BY played_at, id) AS prev_played_at
    FROM valid_plays
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_played_at IS NULL OR (UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(prev_played_at)) / 60 > 30 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY source ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
with_activity AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM sessionized s
    WHERE s.listener_id IN (
            SELECT p.listener_id
            FROM plays p
            WHERE p.is_skipped = TRUE
              AND p.device NOT IN ('tv', 'desktop')
        )
),
scored AS (
    -- Add calculated columns
    SELECT *, SUBSTRING(TRIM(source), 1, 4) AS source_prefix
    FROM with_activity
)
SELECT
    id,
    listener_id,
    played_at,
    ms_played,
    starts_session,
    session_no,
    source_prefix
FROM scored
ORDER BY starts_session DESC
