WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        FIRST_VALUE(event_at) OVER (ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER w_all_by_event_at)) / 60, 1) AS minutes_since_prev_event_at,
        LEAD(event_at) OVER (ORDER BY event_at DESC, id) AS next_event_at,
        ROUND(CUME_DIST() OVER (ORDER BY event_at DESC, id), 3) AS event_at_cume_dist,
        DENSE_RANK() OVER w_all_by_event_at_ties AS event_at_dense_rank
    FROM prepared
    WINDOW w_all_by_event_at AS (ORDER BY event_at DESC, id), w_all_by_event_at_ties AS (ORDER BY event_at DESC)
),
joined_ticket AS (
    -- Join tickets
    SELECT
        s.*,
        t.closed_at AS ticket_closed_at,
        t.opened_at AS ticket_opened_at,
        t.agent_id AS ticket_agent_id
    FROM windowed s
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
annotated AS (
    -- Add calculated columns
    SELECT *, SUBSTRING(TRIM(event_type), 1, 4) AS event_type_prefix
    FROM joined_ticket
    WHERE HOUR(next_event_at) BETWEEN 6 AND 9
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    actor,
    first_event_at,
    minutes_since_prev_event_at,
    next_event_at,
    event_at_cume_dist,
    event_at_dense_rank,
    ticket_closed_at,
    ticket_agent_id,
    event_type_prefix,
    COALESCE(event_at_cume_dist, 0.0) AS event_at_cume_dist_filled,
    COALESCE(event_at_dense_rank, 0) AS event_at_dense_rank_filled
FROM annotated
WHERE event_at_dense_rank BETWEEN 3 AND 7
ORDER BY event_at_dense_rank
