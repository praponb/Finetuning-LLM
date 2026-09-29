WITH filtered AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
),
with_prev AS (
    -- Previous event_at per event_type
    SELECT *, LAG(event_at) OVER (PARTITION BY event_type ORDER BY event_at, id) AS prev_event_at
    FROM filtered
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR (UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(prev_event_at)) / 60 > 60 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY event_type ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        event_type,
        burst_id,
        MIN(event_at) AS session_start,
        MAX(event_at) AS session_end,
        COUNT(*) AS events,
        ROUND((UNIX_TIMESTAMP(MAX(event_at)) - UNIX_TIMESTAMP(MIN(event_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY event_type, burst_id
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(duration_min) OVER (PARTITION BY burst_id, events), 2) AS burst_id_events_avg_duration_min,
        COUNT(*) OVER (PARTITION BY burst_id, events) AS rows_in_burst_id_events,
        SUM(burst_id) OVER (PARTITION BY burst_id ORDER BY session_end DESC, event_type, burst_id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS burst_id_sum_6_prior,
        DENSE_RANK() OVER (PARTITION BY burst_id ORDER BY session_end DESC, event_type, burst_id) AS session_end_dense_rank,
        RANK() OVER (PARTITION BY burst_id ORDER BY session_end DESC, event_type, burst_id) AS session_end_rank
    FROM per_session
),
derived AS (
    -- Add calculated columns
    SELECT *, DATEDIFF(DATE '2025-06-30', session_end) AS days_since_session_end
    FROM sequenced
)
-- Final projection
SELECT
    event_type,
    session_start,
    rows_in_burst_id_events,
    burst_id_sum_6_prior,
    session_end_dense_rank,
    days_since_session_end
FROM derived
WHERE days_since_session_end > 280
ORDER BY burst_id_sum_6_prior
