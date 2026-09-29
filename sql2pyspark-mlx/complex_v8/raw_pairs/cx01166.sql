WITH recent AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_FORMAT(event_at, 'yyyy-MM-dd HH:00') AS event_at_label,
        DATEDIFF(DATE '2025-06-30', event_at) AS days_since_event_at,
        CONCAT_WS(': ', actor, event_type) AS actor_event_type_label
    FROM ticket_events
    WHERE ticket_id <= 6
),
unique_rows AS (
    -- Keep the earliest row per actor, ticket_id
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_at_label,
        days_since_event_at,
        actor_event_type_label
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY actor, ticket_id ORDER BY event_at, id) AS row_num
        FROM recent
    ) ranked
    WHERE row_num = 1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        SIZE(COLLECT_SET(actor) OVER (PARTITION BY event_type, ticket_id)) AS distinct_actor_in_event_type_ticket_id,
        MIN(days_since_event_at) OVER w_event_type_ticket_id AS event_type_ticket_id_min_days_since_event_at,
        FIRST_VALUE(event_at) OVER (ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        ROUND(days_since_event_at / NULLIF(SUM(days_since_event_at) OVER (PARTITION BY event_type, ticket_id), 0), 4) AS days_since_event_at_share_of_event_type_ticket_i
    FROM unique_rows
    WINDOW w_event_type_ticket_id AS (PARTITION BY event_type, ticket_id)
)
-- Subtotals with GROUPING SETS
SELECT
    event_type,
    distinct_actor_in_event_type_ticket_id,
    ROUND(SUM(days_since_event_at) / NULLIF(SUM(distinct_actor_in_event_type_ticket_id), 0), 3) AS days_since_event_at_to_distinct_actor_in_event_t
FROM metrics
GROUP BY GROUPING SETS ((event_type, distinct_actor_in_event_type_ticket_id), (event_type), (distinct_actor_in_event_type_ticket_id), ())
ORDER BY event_type NULLS LAST, distinct_actor_in_event_type_ticket_id NULLS LAST
