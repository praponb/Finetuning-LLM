WITH valid AS (
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
    -- Previous event_at per ticket_id
    SELECT *, LAG(event_at) OVER (PARTITION BY ticket_id ORDER BY event_at, id) AS prev_event_at
    FROM valid
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR event_at > prev_event_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY ticket_id ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
joined AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.new_session_flag,
        t.channel AS ticket_channel,
        t.opened_at AS ticket_opened_at
    FROM sessionized s
    JOIN tickets t ON t.id = s.ticket_id
    WHERE t.channel <> 'chat'
)
SELECT
    new_session_flag,
    event_type,
    ticket_id,
    ARRAY_SORT(COLLECT_SET(event_type)) AS event_type_list,
    COUNT(DISTINCT ticket_id) AS distinct_ticket_ids,
    MIN(new_session_flag) AS min_new_session_flag
FROM joined
GROUP BY CUBE(new_session_flag, event_type, ticket_id)
