WITH scoped AS (
    -- Filter ticket_events
    SELECT *
    FROM ticket_events
    WHERE ticket_id >= 33
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        ticket_id,
        event_at,
        actor,
        RANK() OVER (ORDER BY event_at DESC, id) AS event_at_rank,
        LEAD(event_at) OVER w_event_type_by_event_at AS next_event_at,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER w_event_type_by_event_at)) / 60, 1) AS minutes_since_prev_event_at,
        ROUND(PERCENT_RANK() OVER w_all_by_event_at_ties, 3) AS event_at_percent_rank
    FROM scoped
    WINDOW w_event_type_by_event_at AS (PARTITION BY event_type ORDER BY event_at DESC, id), w_all_by_event_at_ties AS (ORDER BY event_at DESC)
),
top_per_group AS (
    -- Top 3 per group
    SELECT *
    FROM with_windows
    WHERE event_at_rank <= 3
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        ticket_id,
        event_at,
        event_at_rank,
        ROUND(AVG(minutes_since_prev_event_at) OVER (PARTITION BY actor ORDER BY event_at DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS minutes_since_prev_event_at_avg_3,
        NTILE(3) OVER (PARTITION BY actor ORDER BY event_at DESC, id) AS event_at_tercile,
        LAG(next_event_at) OVER (PARTITION BY actor ORDER BY event_at DESC, id) AS prev_next_event_at
    FROM top_per_group
)
SELECT id, ticket_id, event_at_rank
FROM sequenced
ORDER BY ticket_id, event_at_rank DESC, id
LIMIT 10
