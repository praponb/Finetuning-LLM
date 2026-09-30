WITH cleaned AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_at < TIMESTAMP '2025-03-02 10:00:00'
),
with_prev AS (
    -- Previous event_at per event_type
    SELECT *, LAG(event_at) OVER (PARTITION BY event_type ORDER BY event_at, id) AS prev_event_at
    FROM cleaned
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR event_at > prev_event_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY event_type ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', event_at) AS event_at_month,
        burst_id,
        is_new_session,
        MAX(burst_id) AS max_burst_id,
        ARRAY_SORT(COLLECT_SET(event_type)) AS event_type_list,
        COUNT(*) AS row_count,
        MAX(event_at) AS last_event_at
    FROM sessionized
    GROUP BY DATE_TRUNC('MONTH', event_at), burst_id, is_new_session
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        QUARTER(last_event_at) AS last_event_at_quarter,
        GREATEST(max_burst_id, is_new_session) AS greatest_max_burst_id_is_new_session
    FROM rollup_base
    WHERE TO_DATE(last_event_at) < DATE '2025-02-12'
)
SELECT
    event_at_month,
    is_new_session,
    max_burst_id,
    event_type_list,
    row_count,
    last_event_at,
    last_event_at_quarter,
    greatest_max_burst_id_is_new_session
FROM scored
