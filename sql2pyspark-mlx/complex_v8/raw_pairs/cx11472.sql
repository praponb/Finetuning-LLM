WITH scoped_plays AS (
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
    -- Previous played_at per listener_id
    SELECT *, LAG(played_at) OVER (PARTITION BY listener_id ORDER BY played_at, id) AS prev_played_at
    FROM scoped_plays
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_played_at IS NULL OR (UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(prev_played_at)) / 60 > 90 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY listener_id ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        LEAD(played_at) OVER w_burst_id_by_prev_played_at AS next_played_at,
        DENSE_RANK() OVER (PARTITION BY burst_id ORDER BY prev_played_at DESC NULLS FIRST, id) AS prev_played_at_dense_rank,
        RANK() OVER (PARTITION BY burst_id ORDER BY prev_played_at DESC NULLS FIRST) AS prev_played_at_rank,
        ROUND(is_new_session / NULLIF(SUM(is_new_session) OVER (PARTITION BY is_new_session), 0), 4) AS is_new_session_share_of_is_new_session,
        LAST_VALUE(played_at) OVER (PARTITION BY burst_id ORDER BY prev_played_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_played_at,
        SIZE(COLLECT_SET(listener_id) OVER (PARTITION BY is_new_session)) AS distinct_listener_id_in_is_new_session
    FROM sessionized
    WINDOW w_burst_id_by_prev_played_at AS (PARTITION BY burst_id ORDER BY prev_played_at DESC NULLS FIRST, id)
)
-- Final projection
SELECT
    id,
    listener_id,
    track_id,
    played_at,
    ms_played,
    is_skipped,
    source,
    prev_played_at,
    is_new_session,
    burst_id,
    prev_played_at_rank,
    is_new_session_share_of_is_new_session,
    distinct_listener_id_in_is_new_session
FROM metrics
ORDER BY id DESC, prev_played_at_rank
