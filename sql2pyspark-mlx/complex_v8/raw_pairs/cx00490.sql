WITH prepared_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        SUBSTRING(TRIM(actor), 1, 3) AS actor_prefix,
        LOWER(REGEXP_REPLACE(TRIM(event_type), ' ', '_')) AS event_type_slug,
        DATE_FORMAT(event_at, 'MMM yyyy') AS event_at_label
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY actor ORDER BY event_at, id), 3) AS event_at_percent_rank,
        LAST_VALUE(event_at) OVER (PARTITION BY event_type, ticket_id ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY event_type, ticket_id ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at
    FROM prepared_events
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        event_at,
        event_type,
        actor,
        actor_prefix,
        event_type_slug,
        event_at_label,
        event_at_percent_rank,
        second_event_at,
        ROUND((UNIX_TIMESTAMP(second_event_at) - UNIX_TIMESTAMP(LAG(second_event_at) OVER w_event_type_ticket_id_by_even_2)) / 60, 1) AS minutes_since_prev_second_event_at,
        SIZE(COLLECT_SET(actor) OVER (PARTITION BY event_type, MONTH(event_at))) AS distinct_actor_in_event_type_event_at_month
    FROM with_windows
    WINDOW w_event_type_ticket_id_by_even_2 AS (PARTITION BY event_type, ticket_id ORDER BY event_at_percent_rank DESC, id)
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric_name, reading
FROM (
    SELECT id, event_at_percent_rank, minutes_since_prev_second_event_at
    FROM sequenced
) src
UNPIVOT (
    reading FOR metric_name IN (event_at_percent_rank, minutes_since_prev_second_event_at)
) unp
ORDER BY id, metric_name
