WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        COALESCE(actor, 'UNKNOWN') AS actor_or_unknown
    FROM ticket_events
    WHERE ticket_id <= 31
      AND event_type IN ('assigned', 'reopened')
),
with_parent_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.actor_or_unknown,
        t.agent_id AS ticket_agent_id,
        t.closed_at AS ticket_closed_at
    FROM recent s
    JOIN tickets t ON t.id = s.ticket_id
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY event_type, ticket_id ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at,
        LAST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY actor ORDER BY ticket_closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at_2,
        ROUND(CUME_DIST() OVER (PARTITION BY actor ORDER BY ticket_closed_at), 3) AS ticket_closed_at_cume_dist,
        ROUND((UNIX_TIMESTAMP(ticket_closed_at) - UNIX_TIMESTAMP(LAG(ticket_closed_at) OVER w_event_type_ticket_id_by_even_2)) / 60, 1) AS minutes_since_prev_ticket_closed_at,
        DENSE_RANK() OVER (PARTITION BY actor ORDER BY ticket_closed_at, id) AS ticket_closed_at_dense_rank,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER w_event_type_ticket_id_by_even_2)) / 60, 1) AS minutes_since_prev_event_at,
        RANK() OVER (PARTITION BY actor_or_unknown ORDER BY ticket_closed_at DESC NULLS FIRST) AS ticket_closed_at_rank,
        RANK() OVER (PARTITION BY event_type, ticket_id ORDER BY event_at, id) AS event_at_rank,
        LEAD(event_at, 2) OVER (PARTITION BY actor_or_unknown ORDER BY ticket_closed_at DESC NULLS FIRST, id) AS next2_event_at,
        LAG(event_at, 3) OVER (PARTITION BY actor_or_unknown ORDER BY ticket_closed_at DESC NULLS FIRST, id) AS prev3_event_at,
        LEAD(ticket_closed_at, 2) OVER (PARTITION BY event_type, ticket_id ORDER BY event_at, id) AS next2_ticket_closed_at,
        NTILE(4) OVER (PARTITION BY event_type, ticket_id ORDER BY event_at, id) AS event_at_quartile,
        LEAD(ticket_closed_at) OVER (PARTITION BY actor_or_unknown ORDER BY ticket_closed_at DESC NULLS FIRST, id) AS next_ticket_closed_at,
        LEAD(event_at) OVER w_event_type_ticket_id_by_even_2 AS next_event_at
    FROM with_parent_ticket
    WINDOW w_event_type_ticket_id_by_even_2 AS (PARTITION BY event_type, ticket_id ORDER BY event_at, id)
),
first_rows AS (
    -- First row per group
    SELECT *
    FROM with_trends
    WHERE ticket_closed_at_dense_rank = 1
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        actor,
        DATE_TRUNC('MONTH', last_known_event_at_2) AS last_known_event_at_2_month,
        ticket_closed_at_rank,
        MIN(last_known_event_at) AS first_last_known_event_at,
        ARRAY_SORT(COLLECT_SET(event_type)) AS event_type_list,
        ROUND(100 * COUNT(CASE WHEN event_type IS NOT NULL THEN 1 END) / COUNT(*), 1) AS pct_event_type_is_not_null
    FROM first_rows
    WHERE ticket_agent_id >= 5
    GROUP BY actor, DATE_TRUNC('MONTH', last_known_event_at_2), ticket_closed_at_rank
)
SELECT
    actor,
    last_known_event_at_2_month,
    ticket_closed_at_rank,
    event_type_list,
    pct_event_type_is_not_null
FROM stats
WHERE ticket_closed_at_rank < 8
