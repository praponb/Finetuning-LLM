WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT(event_type, '#', CAST(ticket_id AS STRING)) AS event_type_ref,
        LENGTH(TRIM(actor)) AS actor_len,
        LENGTH(TRIM(actor)) AS actor_len_2
    FROM ticket_events
    WHERE event_at >= TIMESTAMP '2025-01-25 11:00:00'
      AND ticket_id IS NOT NULL
),
joined AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.event_type_ref,
        s.actor_len,
        t.tags AS ticket_tags,
        t.opened_at AS ticket_opened_at
    FROM filtered s
    JOIN tickets t ON t.id = s.ticket_id
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE t.actor IS NOT NULL) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_actor_count,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_count,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS last_event_at
    FROM joined s
)
SELECT
    event_type,
    actor_len,
    actor,
    ROUND(SUM(event_actor_count) / NULLIF(SUM(actor_len), 0), 3) AS event_actor_count_to_actor_len_ratio,
    ARRAY_SORT(COLLECT_SET(actor)) AS actor_list,
    MIN(event_at) AS first_event_at
FROM summarized_rows
GROUP BY ROLLUP(event_type, actor_len, actor)
