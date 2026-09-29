WITH prepared_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT_WS('|', event_type, actor) AS event_type_actor_label,
        LENGTH(TRIM(actor)) AS actor_len,
        LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug
    FROM ticket_events
    WHERE (ticket_id IS NOT NULL OR MONTH(event_at) IN (1, 3))
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(actor_len) OVER (PARTITION BY actor ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_actor_len_so_far,
        SUM(actor_len) OVER (ORDER BY event_at, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS actor_len_sum_2,
        FIRST_VALUE(event_at) OVER (PARTITION BY actor ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at
    FROM prepared_events
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_count,
        (SELECT COUNT(CASE WHEN t.event_type IS NOT NULL THEN 1 END) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_event_type_count
    FROM metrics s
)
SELECT id, field, reading
FROM (
    SELECT id, actor_len_sum_2, event_event_type_count, max_actor_len_so_far, event_count
    FROM summarized_rows
) src
UNPIVOT (
    reading FOR field IN (actor_len_sum_2, event_event_type_count, max_actor_len_so_far, event_count)
) unp
