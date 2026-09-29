WITH filtered_plays AS (
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
        UPPER(TRIM(source)) AS source_clean
    FROM plays
),
with_prev AS (
    -- Previous played_at per listener_id
    SELECT *, LAG(played_at) OVER (PARTITION BY listener_id ORDER BY played_at, id) AS prev_played_at
    FROM filtered_plays
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_played_at IS NULL OR (UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(prev_played_at)) / 60 > 45 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY listener_id ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        listener_id,
        session_no,
        MIN(played_at) AS session_start,
        MAX(played_at) AS session_end,
        COUNT(*) AS event_count,
        ROUND((UNIX_TIMESTAMP(MAX(played_at)) - UNIX_TIMESTAMP(MIN(played_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY listener_id, session_no
),
profiled AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM plays p WHERE p.listener_id = s.listener_id) AS play_count,
        (SELECT MAX(p.played_at) FROM plays p WHERE p.listener_id = s.listener_id) AS last_played_at
    FROM sessions s
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        DATE_FORMAT(last_played_at, 'MMM yyyy') AS last_played_at_label,
        DATE_FORMAT(session_end, 'yyyy-MM') AS session_end_label
    FROM profiled
)
-- Native SQL UNPIVOT (wide to long)
SELECT listener_id, session_no, field, metric_value
FROM (
    SELECT listener_id, session_no, event_count, play_count
    FROM derived
) src
UNPIVOT (
    metric_value FOR field IN (event_count, play_count)
) unp
