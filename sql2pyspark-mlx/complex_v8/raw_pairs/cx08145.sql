WITH filtered AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE (ticket_id IS NOT NULL OR event_type = 'created')
),
combined_details_ticket AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        t.channel AS ticket_channel,
        t.opened_at AS ticket_opened_at,
        t.closed_at AS ticket_closed_at
    FROM filtered s
    JOIN tickets t ON t.id = s.ticket_id
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(ticket_opened_at) OVER (PARTITION BY YEAR(ticket_closed_at), actor ORDER BY ticket_opened_at DESC, id) AS prev_ticket_opened_at,
        ROUND(PERCENT_RANK() OVER w_ticket_channel_by_ticket_clo, 3) AS ticket_closed_at_percent_rank,
        NTH_VALUE(ticket_opened_at, 2) OVER (PARTITION BY TO_DATE(ticket_opened_at) ORDER BY ticket_closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ticket_opened_at,
        LAST_VALUE(ticket_closed_at) OVER (PARTITION BY TO_DATE(ticket_opened_at) ORDER BY ticket_closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_ticket_closed_at,
        FIRST_VALUE(ticket_closed_at) IGNORE NULLS OVER (PARTITION BY ticket_channel ORDER BY ticket_closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_ticket_closed_at,
        LAG(ticket_closed_at) OVER (PARTITION BY YEAR(ticket_closed_at), actor ORDER BY ticket_opened_at DESC, id) AS prev_ticket_closed_at,
        ROUND(CUME_DIST() OVER w_ticket_opened_at_to_date_by_2, 3) AS ticket_closed_at_cume_dist,
        RANK() OVER (PARTITION BY YEAR(ticket_closed_at), actor ORDER BY ticket_opened_at DESC, id) AS ticket_opened_at_rank,
        RANK() OVER w_event_type_ticket_channel_by AS ticket_opened_at_rank_2,
        ROUND((UNIX_TIMESTAMP(ticket_opened_at) - UNIX_TIMESTAMP(LAG(ticket_opened_at) OVER (PARTITION BY ticket_id ORDER BY ticket_closed_at, id))) / 60, 1) AS minutes_since_prev_ticket_opened_at
    FROM combined_details_ticket
    WINDOW w_ticket_channel_by_ticket_clo AS (PARTITION BY ticket_channel ORDER BY ticket_closed_at DESC), w_ticket_opened_at_to_date_by_2 AS (PARTITION BY TO_DATE(ticket_opened_at) ORDER BY ticket_closed_at DESC, id), w_event_type_ticket_channel_by AS (PARTITION BY event_type, ticket_channel ORDER BY ticket_opened_at)
),
first_rows AS (
    -- First row per group
    SELECT *
    FROM ranked
    WHERE ticket_opened_at_rank_2 = 1
)
SELECT
    id,
    ticket_id,
    event_type,
    actor,
    ticket_opened_at,
    ticket_closed_at,
    prev_ticket_opened_at,
    ticket_closed_at_percent_rank,
    last_ticket_closed_at,
    ticket_closed_at_cume_dist,
    ticket_opened_at_rank,
    ticket_opened_at_rank_2,
    SUBSTRING(TRIM(actor), 1, 4) AS actor_prefix,
    TO_DATE(prev_ticket_opened_at) AS prev_ticket_opened_at_date
FROM first_rows
WHERE prev_ticket_opened_at < TIMESTAMP '2025-03-08 11:00:00'
ORDER BY ticket_opened_at_rank DESC NULLS FIRST, actor DESC
