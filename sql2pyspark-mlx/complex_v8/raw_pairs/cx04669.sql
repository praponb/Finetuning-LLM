WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE ticket_id >= 50
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT(actor, '#', CAST(ticket_id AS STRING)) AS actor_ref,
        DATE_FORMAT(event_at, 'yyyy-MM') AS event_at_label
    FROM scoped
    WHERE event_at < TIMESTAMP '2025-02-24 13:00:00'
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        LENGTH(TRIM(actor_ref)) AS actor_ref_len,
        DATE_TRUNC('HOUR', event_at) AS event_at_hour,
        CONCAT_WS('|', event_type, event_at_label) AS event_type_event_at_label_label
    FROM annotated
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(actor_ref_len) OVER (ORDER BY actor_ref_len ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_actor_ref_len_so_far,
        NTH_VALUE(event_at, 2) OVER (ORDER BY actor_ref_len ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        SUM(actor_ref_len) OVER w_event_type AS event_type_sum_actor_ref_len,
        DENSE_RANK() OVER (PARTITION BY ticket_id ORDER BY event_at_hour DESC) AS event_at_hour_dense_rank,
        LAST_VALUE(actor_ref_len) IGNORE NULLS OVER (PARTITION BY ticket_id ORDER BY event_at_hour DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_actor_ref_len
    FROM labeled
    WINDOW w_event_type AS (PARTITION BY event_type)
),
labeled_2 AS (
    -- Label and score rows
    SELECT
        id,
        actor,
        actor_ref,
        event_at_label,
        actor_ref_len,
        event_at_hour,
        min_actor_ref_len_so_far,
        event_at_hour_dense_rank,
        ROUND((UNIX_TIMESTAMP(second_event_at) - UNIX_TIMESTAMP(event_at_hour)) / 3600, 1) AS hours_event_at_hour_to_second_event_at
    FROM windowed
)
SELECT
    id,
    actor,
    actor_ref,
    event_at_label,
    actor_ref_len,
    event_at_hour,
    min_actor_ref_len_so_far,
    event_at_hour_dense_rank,
    hours_event_at_hour_to_second_event_at,
    COALESCE(actor_ref_len, 0) AS actor_ref_len_filled,
    LOWER(REGEXP_REPLACE(TRIM(actor_ref), ' ', '_')) AS actor_ref_slug
FROM labeled_2
ORDER BY actor_ref_slug DESC NULLS FIRST, event_at_label DESC
