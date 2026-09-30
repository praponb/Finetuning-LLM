WITH cleaned AS (
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
combined_details_ticket AS (
    -- Join tickets
    SELECT s.*, t.opened_at AS ticket_opened_at, t.channel AS ticket_channel
    FROM cleaned s
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTH_VALUE(ticket_opened_at, 2) OVER (PARTITION BY ticket_id ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ticket_opened_at,
        LEAD(ticket_opened_at) OVER w_actor_or_unknown_by_event_at AS next_ticket_opened_at,
        LAG(ticket_opened_at) OVER (PARTITION BY ticket_id ORDER BY event_at, id) AS prev_ticket_opened_at,
        LAST_VALUE(ticket_opened_at) OVER (PARTITION BY MONTH(event_at) ORDER BY ticket_opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_ticket_opened_at,
        ROW_NUMBER() OVER w_event_at_month_by_ticket_ope_2 AS ticket_opened_at_seq,
        ROUND(CUME_DIST() OVER (PARTITION BY ticket_id ORDER BY event_at, id), 3) AS event_at_cume_dist,
        FIRST_VALUE(event_at) OVER (PARTITION BY actor_or_unknown ORDER BY event_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        DENSE_RANK() OVER (PARTITION BY ticket_id ORDER BY event_at, id) AS event_at_dense_rank,
        DENSE_RANK() OVER (PARTITION BY ticket_id ORDER BY event_at) AS event_at_dense_rank_2,
        RANK() OVER (PARTITION BY ticket_id ORDER BY event_at, id) AS event_at_rank,
        ROUND((UNIX_TIMESTAMP(ticket_opened_at) - UNIX_TIMESTAMP(LAG(ticket_opened_at) OVER (PARTITION BY ticket_id ORDER BY event_at, id))) / 60, 1) AS minutes_since_prev_ticket_opened_at,
        RANK() OVER (PARTITION BY actor_or_unknown ORDER BY event_at NULLS LAST) AS event_at_rank_2
    FROM combined_details_ticket
    WINDOW w_actor_or_unknown_by_event_at AS (PARTITION BY actor_or_unknown ORDER BY event_at NULLS LAST, id), w_event_at_month_by_ticket_ope_2 AS (PARTITION BY MONTH(event_at) ORDER BY ticket_opened_at, id)
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        ticket_opened_at,
        ticket_channel,
        second_ticket_opened_at,
        next_ticket_opened_at,
        last_ticket_opened_at,
        ticket_opened_at_seq,
        event_at_cume_dist,
        first_event_at,
        event_at_dense_rank,
        event_at_rank,
        minutes_since_prev_ticket_opened_at,
        event_at_rank_2,
        ROUND(event_at_cume_dist * 1.1, 2) AS event_at_cume_dist_adj
    FROM with_trends
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_type,
    ticket_channel,
    next_ticket_opened_at,
    last_ticket_opened_at,
    first_event_at,
    event_at_dense_rank,
    minutes_since_prev_ticket_opened_at,
    event_at_cume_dist_adj,
    SUBSTRING(TRIM(ticket_channel), 1, 2) AS ticket_channel_prefix,
    CASE WHEN actor IN ('customer', 'system') THEN 'group_a' WHEN actor = 'agent' THEN 'group_b' ELSE 'other' END AS actor_group
FROM annotated
