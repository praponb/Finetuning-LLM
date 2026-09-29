WITH base AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_type IN ('escalated', 'resolved')
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        ticket_id,
        event_type,
        actor,
        NTILE(3) OVER w_ticket_id_event_type_by_even AS event_at_tercile,
        LAG(event_at) OVER w_ticket_id_event_type_by_even AS prev_event_at,
        FIRST_VALUE(event_at) OVER (PARTITION BY ticket_id, event_type ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        DENSE_RANK() OVER (PARTITION BY ticket_id, event_type ORDER BY event_at, id) AS event_at_dense_rank,
        LAG(event_at, 3) OVER (PARTITION BY ticket_id, event_type ORDER BY event_at, id) AS prev3_event_at,
        DENSE_RANK() OVER w_ticket_id_event_type_by_even_3 AS event_at_dense_rank_2
    FROM base
    WINDOW w_ticket_id_event_type_by_even AS (PARTITION BY ticket_id, event_type ORDER BY event_at, id), w_ticket_id_event_type_by_even_3 AS (PARTITION BY ticket_id, event_type ORDER BY event_at)
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        event_type,
        DATE_TRUNC('MONTH', prev3_event_at) AS prev3_event_at_month,
        event_at_tercile,
        ARRAY_SORT(COLLECT_SET(actor)) AS actor_list,
        SUM(event_at_dense_rank_2) AS total_event_at_dense_rank_2,
        SUM(CASE WHEN actor IN ('agent', 'customer') THEN 1 ELSE 0 END) AS n_actor_in_agent_customer,
        MIN(prev3_event_at) AS first_prev3_event_at
    FROM ranked
    GROUP BY event_type, DATE_TRUNC('MONTH', prev3_event_at), event_at_tercile
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY event_type, event_at_tercile ORDER BY first_prev3_event_at, event_type, prev3_event_at_month, event_at_tercile) AS first_prev3_event_at_dense_rank,
        RANK() OVER w_event_at_tercile_first_prev3 AS n_actor_in_agent_customer_rank
    FROM grouped
    WINDOW w_event_at_tercile_first_prev3 AS (PARTITION BY event_at_tercile, MONTH(first_prev3_event_at) ORDER BY n_actor_in_agent_customer, event_type, prev3_event_at_month, event_at_tercile)
)
SELECT
    event_type,
    prev3_event_at_month,
    actor_list,
    total_event_at_dense_rank_2,
    n_actor_in_agent_customer,
    first_prev3_event_at,
    first_prev3_event_at_dense_rank,
    n_actor_in_agent_customer_rank
FROM with_trends
WHERE n_actor_in_agent_customer_rank >= 3
ORDER BY n_actor_in_agent_customer DESC NULLS FIRST, n_actor_in_agent_customer_rank
