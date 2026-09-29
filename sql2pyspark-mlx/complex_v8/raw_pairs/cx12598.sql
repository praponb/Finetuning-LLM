WITH base AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_type IN ('assigned', 'resolved')
      AND event_at >= TIMESTAMP '2025-01-31 13:00:00'
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LAST_VALUE(event_at) OVER (PARTITION BY event_type ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at,
        NTILE(10) OVER (PARTITION BY MONTH(event_at) ORDER BY event_at, id) AS event_at_decile,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY event_type ORDER BY event_at DESC, id))) / 60, 1) AS minutes_since_prev_event_at,
        NTILE(10) OVER (PARTITION BY event_type ORDER BY event_at DESC, id) AS event_at_decile_2,
        ROW_NUMBER() OVER w_event_type_by_event_at AS event_at_seq,
        RANK() OVER (PARTITION BY MONTH(event_at) ORDER BY event_at) AS event_at_rank
    FROM base
    WINDOW w_event_type_by_event_at AS (PARTITION BY event_type ORDER BY event_at DESC, id)
),
qualified AS (
    -- Keep rows whose ticket matches
    SELECT s.*
    FROM windowed s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.id = s.ticket_id
              AND t.csat >= 1
              AND t.priority = 'P4'
        )
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        last_event_at,
        event_at_decile,
        minutes_since_prev_event_at,
        event_at_decile_2,
        event_at_seq,
        event_at_rank,
        LAST_VALUE(minutes_since_prev_event_at) OVER (PARTITION BY MONTH(last_event_at), event_at_rank ORDER BY minutes_since_prev_event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_minutes_since_prev_event_at,
        LEAD(event_at_decile_2) OVER (PARTITION BY MONTH(last_event_at), event_at_rank ORDER BY minutes_since_prev_event_at ASC, id) AS next_event_at_decile_2
    FROM qualified
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    last_event_at,
    event_at_decile,
    minutes_since_prev_event_at,
    event_at_decile_2,
    event_at_seq,
    event_at_rank,
    last_minutes_since_prev_event_at,
    next_event_at_decile_2,
    GREATEST(event_at_rank, next_event_at_decile_2) AS greatest_event_at_rank_next_event_at_decile_2
FROM ranked
ORDER BY event_at_seq DESC, event_at_rank DESC NULLS FIRST
