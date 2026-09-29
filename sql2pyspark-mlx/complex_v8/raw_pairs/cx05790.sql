WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_type <> 'escalated'
      AND ticket_id <= 51
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LAG(event_at, 2) OVER (PARTITION BY event_type ORDER BY event_at DESC, id) AS prev2_event_at,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY event_type ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        DENSE_RANK() OVER (PARTITION BY event_type ORDER BY event_at DESC, id) AS event_at_dense_rank
    FROM recent
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        ticket_id,
        actor,
        COUNT(*) FILTER (WHERE actor = 'customer') AS n_actor_customer,
        MIN(event_at_dense_rank) AS min_event_at_dense_rank,
        MAX(second_event_at) AS last_second_event_at
    FROM with_windows
    GROUP BY ticket_id, actor
),
detail_rows AS (
    -- One row per event
    SELECT
        s.ticket_id,
        s.actor,
        s.n_actor_customer,
        s.min_event_at_dense_rank,
        s.last_second_event_at,
        t.id AS event_id,
        t.event_at,
        t.actor AS event_actor,
        t.event_type
    FROM grouped s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id
),
latest_per_key AS (
    -- Keep the earliest row per event_type, event_actor
    SELECT
        ticket_id,
        actor,
        n_actor_customer,
        min_event_at_dense_rank,
        last_second_event_at,
        event_id,
        event_at,
        event_actor,
        event_type
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY event_type, event_actor ORDER BY last_second_event_at, ticket_id, actor, event_id) AS row_num
        FROM detail_rows
    ) ranked
    WHERE row_num = 1
),
with_parent_event AS (
    -- Attach event attributes
    SELECT
        s.ticket_id,
        s.actor,
        s.n_actor_customer,
        s.min_event_at_dense_rank,
        s.event_id,
        s.event_actor,
        s.event_type,
        t.event_at AS event_event_at,
        t.actor AS event_actor_2
    FROM latest_per_key s
    LEFT JOIN ticket_events t ON t.id = s.event_id AND t.event_type NOT IN ('created', 'reopened')
)
SELECT
    event_type,
    actor,
    COUNT(*) AS cnt,
    SUM(n_actor_customer) AS total_n_actor_customer
FROM with_parent_event
GROUP BY ROLLUP(event_type, actor)
ORDER BY event_type NULLS LAST, actor NULLS LAST
