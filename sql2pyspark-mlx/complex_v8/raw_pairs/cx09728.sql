WITH filtered_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        SUBSTRING(TRIM(actor), 1, 2) AS actor_prefix,
        LOWER(REGEXP_REPLACE(TRIM(event_type), ' ', '_')) AS event_type_slug
    FROM ticket_events
    WHERE actor IN ('customer', 'system')
      AND TO_DATE(event_at) < DATE '2025-02-06'
),
enriched_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.actor_prefix,
        t.tags AS ticket_tags,
        t.closed_at AS ticket_closed_at,
        t.opened_at AS ticket_opened_at,
        su.agent_name,
        su.tier AS agent_tier
    FROM filtered_events s
    JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (PARTITION BY TO_DATE(ticket_opened_at) ORDER BY event_at DESC, id) AS event_at_rank,
        NTH_VALUE(ticket_opened_at, 2) OVER (ORDER BY event_at ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ticket_opened_at,
        DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', ticket_opened_at), ticket_id ORDER BY ticket_closed_at, id) AS ticket_closed_at_dense_rank,
        DENSE_RANK() OVER (ORDER BY event_at ASC NULLS LAST) AS event_at_dense_rank,
        NTILE(4) OVER (PARTITION BY actor ORDER BY ticket_closed_at, id) AS ticket_closed_at_quartile,
        RANK() OVER w_ticket_opened_at_month_ticke_2 AS ticket_closed_at_rank,
        NTH_VALUE(ticket_closed_at, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', ticket_opened_at), ticket_id ORDER BY ticket_closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ticket_closed_at,
        FIRST_VALUE(event_at) OVER (ORDER BY event_at ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        ROW_NUMBER() OVER (ORDER BY event_at ASC NULLS LAST, id) AS event_at_seq,
        ROUND(CUME_DIST() OVER (PARTITION BY actor ORDER BY ticket_closed_at, id), 3) AS ticket_closed_at_cume_dist,
        ROUND((UNIX_TIMESTAMP(ticket_opened_at) - UNIX_TIMESTAMP(LAG(ticket_opened_at) OVER w_ticket_closed_at_month_actor)) / 60, 1) AS minutes_since_prev_ticket_opened_at,
        RANK() OVER w_all_by_event_at AS event_at_rank_2
    FROM enriched_ticket
    WINDOW w_ticket_opened_at_month_ticke_2 AS (PARTITION BY DATE_TRUNC('MONTH', ticket_opened_at), ticket_id ORDER BY ticket_closed_at), w_ticket_closed_at_month_actor AS (PARTITION BY DATE_TRUNC('MONTH', ticket_closed_at), actor ORDER BY ticket_opened_at DESC, id), w_all_by_event_at AS (ORDER BY event_at ASC NULLS LAST, id)
),
leaders AS (
    -- Top 5 per group
    SELECT *
    FROM with_windows
    WHERE event_at_rank_2 <= 5
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(second_ticket_opened_at) IGNORE NULLS OVER (PARTITION BY event_at_rank, YEAR(second_ticket_opened_at) ORDER BY second_ticket_closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_second_ticket_opened_at,
        COUNT(*) OVER (PARTITION BY event_at_seq, event_at_rank_2) AS rows_in_event_at_seq_event_at_rank_2,
        event_at_seq - LAG(event_at_seq) OVER (PARTITION BY event_at_rank, YEAR(second_ticket_opened_at) ORDER BY second_ticket_closed_at DESC, id) AS event_at_seq_change,
        COUNT(*) OVER (PARTITION BY event_at_rank, ticket_closed_at_rank) AS rows_in_event_at_rank_ticket_closed_at_rank,
        LAST_VALUE(ticket_closed_at_dense_rank) OVER (PARTITION BY agent_tier ORDER BY event_at_rank_2 DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_ticket_closed_at_dense_rank,
        NTILE(10) OVER (PARTITION BY event_at_seq, ticket_id ORDER BY first_event_at, id) AS first_event_at_decile
    FROM leaders
)
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    actor,
    actor_prefix,
    ticket_tags,
    ticket_opened_at,
    agent_tier,
    event_at_rank,
    second_ticket_opened_at,
    event_at_dense_rank,
    ticket_closed_at_rank,
    second_ticket_closed_at,
    first_event_at,
    event_at_seq,
    ticket_closed_at_cume_dist,
    minutes_since_prev_ticket_opened_at,
    event_at_rank_2,
    first_known_second_ticket_opened_at,
    rows_in_event_at_seq_event_at_rank_2,
    event_at_seq_change,
    rows_in_event_at_rank_ticket_closed_at_rank,
    last_ticket_closed_at_dense_rank,
    first_event_at_decile
FROM with_trends
ORDER BY event_at NULLS LAST
