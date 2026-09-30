WITH filtered_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        COALESCE(actor, 'UNKNOWN') AS actor_or_unknown
    FROM ticket_events
    WHERE event_type <> 'created'
),
with_prev AS (
    -- Previous event_at per actor
    SELECT *, LAG(event_at) OVER (PARTITION BY actor ORDER BY event_at, id) AS prev_event_at
    FROM filtered_events
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR (UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(prev_event_at)) / 60 > 45 THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY actor ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        actor,
        session_id,
        MIN(event_at) AS session_start,
        MAX(event_at) AS session_end,
        COUNT(*) AS events,
        ROUND((UNIX_TIMESTAMP(MAX(event_at)) - UNIX_TIMESTAMP(MIN(event_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY actor, session_id
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        actor,
        session_id,
        session_start,
        events,
        duration_min,
        COUNT(*) OVER (PARTITION BY session_id) AS rows_in_session_id,
        FIRST_VALUE(events) OVER (PARTITION BY session_id, events ORDER BY duration_min DESC NULLS FIRST, actor, session_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_events,
        ROUND(PERCENT_RANK() OVER w_session_id_events_by_duratio_2, 3) AS duration_min_percent_rank,
        SIZE(COLLECT_SET(actor) OVER w_session_id) AS distinct_actor_in_session_id,
        MAX(duration_min) OVER (PARTITION BY session_id) AS session_id_max_duration_min
    FROM per_session
    WINDOW w_session_id_events_by_duratio_2 AS (PARTITION BY session_id, events ORDER BY duration_min DESC NULLS FIRST), w_session_id AS (PARTITION BY session_id)
)
SELECT actor, session_id, metric, metric_value
FROM (
    SELECT actor, session_id, duration_min, duration_min_percent_rank
    FROM ranked
) src
UNPIVOT (
    metric_value FOR metric IN (duration_min, duration_min_percent_rank)
) unp
