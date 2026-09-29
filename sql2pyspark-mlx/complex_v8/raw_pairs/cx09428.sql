WITH filtered_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT_WS('-', event_type, actor) AS event_type_actor_label
    FROM ticket_events
),
with_prev AS (
    -- Previous event_at per actor
    SELECT *, LAG(event_at) OVER (PARTITION BY actor ORDER BY event_at, id) AS prev_event_at
    FROM filtered_events
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR (UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(prev_event_at)) / 60 > 60 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY actor ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
summary AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', event_at) AS event_at_month,
        ticket_id,
        SUM(starts_session) AS total_starts_session,
        COUNT(DISTINCT actor) AS distinct_actors,
        SUM(CASE WHEN event_type NOT IN ('created', 'resolved') THEN 1 ELSE 0 END) AS n_event_type_not_in_created_re,
        COUNT(*) AS row_count
    FROM sessionized
    GROUP BY 1, 2
    HAVING AVG(session_no) > 2
),
qualified AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM summary s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.event_type IN ('assigned', 'reopened')
        )
)
SELECT event_at_month, ticket_id, distinct_actors
FROM qualified
WHERE n_event_type_not_in_created_re BETWEEN 3 AND 8
ORDER BY event_at_month DESC, distinct_actors DESC
