WITH scoped_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        SUBSTRING(TRIM(event_type), 1, 3) AS event_type_prefix
    FROM ticket_events
),
with_parent_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type_prefix,
        t.agent_id AS ticket_agent_id,
        t.closed_at AS ticket_closed_at
    FROM scoped_events s
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(CUME_DIST() OVER w_event_at_year_ticket_agent_i, 3) AS event_at_cume_dist,
        DENSE_RANK() OVER (ORDER BY event_at ASC, id) AS event_at_dense_rank,
        LAG(event_at, 3) OVER (PARTITION BY ticket_agent_id, TO_DATE(ticket_closed_at) ORDER BY event_at DESC, id) AS prev3_event_at,
        LEAD(event_at, 3) OVER (PARTITION BY ticket_agent_id, ticket_id ORDER BY ticket_closed_at DESC, id) AS next3_event_at,
        LEAD(ticket_closed_at) OVER (PARTITION BY ticket_id, ticket_agent_id ORDER BY event_at DESC NULLS FIRST, id) AS next_ticket_closed_at,
        ROW_NUMBER() OVER w_ticket_agent_id_ticket_close AS rn,
        RANK() OVER (PARTITION BY ticket_id, ticket_agent_id ORDER BY event_at DESC NULLS FIRST, id) AS event_at_rank,
        RANK() OVER (PARTITION BY ticket_agent_id, TO_DATE(ticket_closed_at) ORDER BY event_at DESC) AS event_at_rank_2,
        ROUND(PERCENT_RANK() OVER (PARTITION BY ticket_agent_id, TO_DATE(ticket_closed_at) ORDER BY event_at DESC, id), 3) AS event_at_percent_rank,
        LAG(ticket_closed_at, 2) OVER (PARTITION BY ticket_id, ticket_agent_id ORDER BY event_at DESC NULLS FIRST, id) AS prev2_ticket_closed_at,
        RANK() OVER (PARTITION BY YEAR(event_at), ticket_agent_id ORDER BY event_at) AS event_at_rank_3,
        ROW_NUMBER() OVER (PARTITION BY YEAR(event_at), ticket_agent_id ORDER BY event_at, id) AS rn_2,
        NTH_VALUE(ticket_closed_at, 2) OVER (PARTITION BY ticket_agent_id, TO_DATE(ticket_closed_at) ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ticket_closed_at,
        DENSE_RANK() OVER w_ticket_id_ticket_agent_id_by AS event_at_dense_rank_2,
        LEAD(event_at, 2) OVER w_ticket_agent_id_ticket_id_by AS next2_event_at
    FROM with_parent_ticket
    WINDOW w_event_at_year_ticket_agent_i AS (PARTITION BY YEAR(event_at), ticket_agent_id ORDER BY event_at, id), w_ticket_agent_id_ticket_close AS (PARTITION BY ticket_agent_id, TO_DATE(ticket_closed_at) ORDER BY event_at DESC, id), w_ticket_id_ticket_agent_id_by AS (PARTITION BY ticket_id, ticket_agent_id ORDER BY event_at DESC NULLS FIRST, id), w_ticket_agent_id_ticket_id_by AS (PARTITION BY ticket_agent_id, ticket_id ORDER BY ticket_closed_at DESC, id)
),
sequenced_2 AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(second_ticket_closed_at) - UNIX_TIMESTAMP(LAG(second_ticket_closed_at) OVER w_next3_event_at_month_event_a)) / 60, 1) AS minutes_since_prev_second_ticket_closed_at,
        MAX(event_at_rank_2) OVER w_ticket_agent_id AS ticket_agent_id_max_event_at_rank_2,
        LEAD(event_at) OVER (PARTITION BY event_at_rank_2 ORDER BY next3_event_at, id) AS next_event_at,
        MAX(event_at_cume_dist) OVER w_ticket_agent_id AS ticket_agent_id_max_event_at_cume_dist
    FROM sequenced
    WINDOW w_next3_event_at_month_event_a AS (PARTITION BY MONTH(next3_event_at), event_at_dense_rank ORDER BY event_at_percent_rank DESC NULLS FIRST, id), w_ticket_agent_id AS (PARTITION BY ticket_agent_id)
)
SELECT
    id,
    ticket_id,
    event_at,
    ticket_closed_at,
    event_at_cume_dist,
    event_at_dense_rank,
    prev3_event_at,
    next_ticket_closed_at,
    rn,
    event_at_rank,
    event_at_rank_2,
    event_at_percent_rank,
    prev2_ticket_closed_at,
    rn_2,
    second_ticket_closed_at,
    next2_event_at,
    minutes_since_prev_second_ticket_closed_at,
    next_event_at,
    ticket_agent_id_max_event_at_cume_dist,
    next2_event_at + INTERVAL 1 DAYS AS next2_event_at_plus_1d,
    DATEDIFF(DATE '2025-06-30', next2_event_at) AS days_since_next2_event_at
FROM sequenced_2
WHERE ticket_closed_at < TIMESTAMP '2025-02-19 21:00:00'
ORDER BY second_ticket_closed_at, id
LIMIT 10
