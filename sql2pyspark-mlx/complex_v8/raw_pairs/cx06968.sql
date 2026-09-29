WITH valid_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT_WS(': ', event_type, actor) AS event_type_actor_label
    FROM ticket_events
),
with_prev AS (
    -- Previous event_at per event_type
    SELECT *, LAG(event_at) OVER (PARTITION BY event_type ORDER BY event_at, id) AS prev_event_at
    FROM valid_events
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_event_at IS NULL OR (UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(prev_event_at)) / 60 > 120 THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY event_type ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        event_type,
        burst_id,
        MIN(event_at) AS session_start,
        MAX(event_at) AS session_end,
        COUNT(*) AS n_events,
        ROUND((UNIX_TIMESTAMP(MAX(event_at)) - UNIX_TIMESTAMP(MIN(event_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY event_type, burst_id
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        LEAD(session_end) OVER w_session_start_year_by_sessio AS next_session_end,
        ROUND(CUME_DIST() OVER (PARTITION BY YEAR(session_start) ORDER BY session_end, event_type, burst_id), 3) AS session_end_cume_dist
    FROM sessions
    WINDOW w_session_start_year_by_sessio AS (PARTITION BY YEAR(session_start) ORDER BY session_end, event_type, burst_id)
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT next_session_end, session_end, event_type, n_events
    FROM metrics
) src
PIVOT (
    COUNT(n_events) AS count_v, SUM(n_events) AS sum_v
    FOR event_type IN ('assigned', 'reopened')
)
ORDER BY next_session_end, session_end
