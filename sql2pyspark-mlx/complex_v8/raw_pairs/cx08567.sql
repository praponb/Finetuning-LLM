WITH base_events AS (
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
    -- Previous event_at per actor
    SELECT *, LAG(event_at) OVER (PARTITION BY actor ORDER BY event_at, id) AS prev_event_at
    FROM base_events
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR (UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(prev_event_at)) / 60 > 30 THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY actor ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
scored AS (
    -- Add calculated columns
    SELECT *, DATE_TRUNC('MONTH', prev_event_at) AS prev_event_at_month
    FROM sessionized
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        ticket_id,
        event_type,
        actor,
        prev_event_at,
        session_no,
        CONCAT_WS('-', event_type, actor) AS event_type_actor_label,
        SUBSTRING(TRIM(event_type), 1, 2) AS event_type_prefix,
        CASE WHEN actor IN ('customer', 'agent') THEN 'group_a' WHEN actor = 'system' THEN 'group_b' ELSE 'other' END AS actor_group
    FROM scored
)
-- Subtotals with GROUPING SETS
SELECT
    session_no,
    ticket_id,
    GROUPING(session_no) AS is_total_session_no,
    ROUND(100 * COUNT(CASE WHEN actor IN ('system', 'agent') THEN 1 END) / COUNT(*), 1) AS pct_actor_in_system_agent
FROM labeled
GROUP BY GROUPING SETS ((session_no, ticket_id), (session_no), ())
