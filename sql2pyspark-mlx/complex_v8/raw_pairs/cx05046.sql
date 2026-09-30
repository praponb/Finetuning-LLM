WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DAYOFWEEK(event_at) AS event_at_dayofweek
    FROM ticket_events
    WHERE event_at >= TIMESTAMP '2025-01-31 13:00:00'
),
with_details_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        t.csat AS ticket_csat
    FROM cleaned s
    JOIN tickets t ON t.id = s.ticket_id
    WHERE t.csat > 2
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        event_at,
        event_type,
        actor,
        ticket_csat,
        DENSE_RANK() OVER (PARTITION BY ticket_csat ORDER BY ticket_csat DESC NULLS FIRST) AS ticket_csat_dense_rank,
        ROUND(ticket_csat / NULLIF(SUM(ticket_csat) OVER (PARTITION BY actor), 0), 4) AS ticket_csat_share_of_actor,
        ROW_NUMBER() OVER w_all_by_ticket_csat AS seq_no,
        NTH_VALUE(ticket_csat, 2) OVER (PARTITION BY ticket_csat ORDER BY ticket_csat DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ticket_csat,
        MIN(ticket_csat) OVER w_actor AS actor_min_ticket_csat,
        SIZE(COLLECT_SET(event_type) OVER (PARTITION BY actor)) AS distinct_event_type_in_actor,
        LEAD(event_at, 2) OVER (PARTITION BY ticket_csat ORDER BY ticket_csat DESC NULLS FIRST, id) AS next2_event_at,
        ROUND(AVG(ticket_csat) OVER (PARTITION BY actor), 2) AS actor_avg_ticket_csat,
        ROW_NUMBER() OVER w_ticket_csat_by_ticket_csat AS seq_no_2,
        MAX(ticket_csat) OVER (PARTITION BY actor) AS actor_max_ticket_csat,
        ROUND(AVG(ticket_csat) OVER (ORDER BY ticket_csat, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS ticket_csat_avg_3
    FROM with_details_ticket
    WINDOW w_all_by_ticket_csat AS (ORDER BY ticket_csat, id), w_actor AS (PARTITION BY actor), w_ticket_csat_by_ticket_csat AS (PARTITION BY ticket_csat ORDER BY ticket_csat DESC NULLS FIRST, id)
),
metrics_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY actor, second_ticket_csat ORDER BY ticket_csat_share_of_actor NULLS LAST, id) AS seq_no_3,
        LAG(actor_min_ticket_csat) OVER w_actor_second_ticket_csat_by AS prev_actor_min_ticket_csat,
        SUM(actor_min_ticket_csat) OVER (ORDER BY next2_event_at DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS actor_min_ticket_csat_sum_1_prior
    FROM metrics
    WINDOW w_actor_second_ticket_csat_by AS (PARTITION BY actor, second_ticket_csat ORDER BY ticket_csat_share_of_actor NULLS LAST, id)
)
-- Final projection
SELECT
    id,
    event_at,
    event_type,
    actor,
    ticket_csat,
    seq_no,
    seq_no_2
FROM metrics_2
ORDER BY ticket_csat
