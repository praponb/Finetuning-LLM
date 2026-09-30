WITH scoped_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
),
combined_details_ticket AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        t.channel AS ticket_channel,
        t.opened_at AS ticket_opened_at
    FROM scoped_events s
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(CUME_DIST() OVER (ORDER BY ticket_opened_at DESC), 3) AS ticket_opened_at_cume_dist,
        NTILE(4) OVER w_all_by_event_at AS event_at_quartile,
        LEAD(ticket_opened_at, 3) OVER (PARTITION BY ticket_id, event_type ORDER BY event_at DESC, id) AS next3_ticket_opened_at,
        RANK() OVER (ORDER BY ticket_opened_at DESC, id) AS ticket_opened_at_rank,
        LEAD(ticket_opened_at) OVER w_all_by_ticket_opened_at AS next_ticket_opened_at,
        ROUND(PERCENT_RANK() OVER (ORDER BY ticket_opened_at DESC), 3) AS ticket_opened_at_percent_rank,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY TO_DATE(event_at) ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        LAST_VALUE(ticket_opened_at) IGNORE NULLS OVER (PARTITION BY TO_DATE(event_at) ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_ticket_opened_at,
        NTH_VALUE(event_at, 2) OVER (ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at_2
    FROM combined_details_ticket
    WINDOW w_all_by_event_at AS (ORDER BY event_at ASC, id), w_all_by_ticket_opened_at AS (ORDER BY ticket_opened_at DESC, id)
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        event_at,
        event_type,
        actor,
        ticket_channel,
        event_at_quartile,
        next3_ticket_opened_at,
        ticket_opened_at_rank,
        next_ticket_opened_at,
        ticket_opened_at_percent_rank,
        second_event_at,
        last_known_ticket_opened_at,
        second_event_at_2,
        CONCAT_WS('-', event_type, actor) AS event_type_actor_label,
        ROUND(ticket_opened_at_rank / NULLIF(event_at_quartile, 0), 3) AS ticket_opened_at_rank_per_event_at_quartile
    FROM with_trends
    WHERE ticket_opened_at_cume_dist < 0.8
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY ticket_opened_at_rank, DATE_TRUNC('MONTH', next3_ticket_opened_at)) AS rows_in_ticket_opened_at_rank_next3_ticket_opene,
        NTILE(10) OVER (ORDER BY next_ticket_opened_at DESC, id) AS next_ticket_opened_at_decile,
        LAG(second_event_at, 2) OVER (PARTITION BY event_at_quartile ORDER BY event_at_quartile DESC, id) AS prev2_second_event_at,
        MIN(event_at_quartile) OVER w_ticket_opened_at_rank_next3 AS ticket_opened_at_rank_next3_ticket_opened_at_mon,
        NTH_VALUE(next_ticket_opened_at, 2) OVER (ORDER BY next_ticket_opened_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_next_ticket_opened_at
    FROM enriched
    WINDOW w_ticket_opened_at_rank_next3 AS (PARTITION BY ticket_opened_at_rank, DATE_TRUNC('MONTH', next3_ticket_opened_at))
)
SELECT
    id,
    event_at,
    event_type,
    actor,
    ticket_channel,
    event_at_quartile,
    next3_ticket_opened_at,
    ticket_opened_at_rank,
    next_ticket_opened_at,
    second_event_at,
    second_event_at_2,
    event_type_actor_label,
    ticket_opened_at_rank_per_event_at_quartile,
    next_ticket_opened_at_decile,
    ticket_opened_at_rank_next3_ticket_opened_at_mon,
    second_next_ticket_opened_at
FROM sequenced
