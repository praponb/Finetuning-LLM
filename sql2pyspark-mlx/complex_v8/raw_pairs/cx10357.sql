WITH cleaned AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LENGTH(TRIM(actor)) AS actor_len,
        CONCAT_WS(': ', event_type, actor) AS event_type_actor_label
    FROM ticket_events
    WHERE (actor NOT IN ('agent', 'customer') OR ticket_id IS NOT NULL)
),
enriched_ticket AS (
    -- Enrich with dimension data
    SELECT s.*, t.requester_id AS ticket_requester_id
    FROM cleaned s
    JOIN tickets t ON t.id = s.ticket_id
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        event_at,
        actor,
        actor_len,
        DATE_FORMAT(event_at, 'yyyy-MM') AS event_at_label,
        FLOOR(ticket_requester_id / 5) * 5 AS ticket_requester_id_bucket,
        ROUND(ticket_requester_id / NULLIF(actor_len, 0), 3) AS ticket_requester_id_per_actor_len
    FROM enriched_ticket
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        TO_DATE(event_at) AS event_at_date,
        CONCAT_WS('-', actor, event_at_label) AS actor_event_at_label_label,
        SUBSTRING(TRIM(actor), 1, 4) AS actor_prefix
    FROM annotated
    WHERE ticket_requester_id_bucket > 8
)
-- Final projection
SELECT
    id,
    event_at,
    actor,
    actor_len,
    event_at_label,
    ticket_requester_id_bucket,
    ticket_requester_id_per_actor_len,
    event_at_date,
    actor_event_at_label_label,
    actor_prefix,
    INITCAP(TRIM(actor_prefix)) AS actor_prefix_clean,
    LOWER(REGEXP_REPLACE(TRIM(event_at_label), ' ', '_')) AS event_at_label_slug
FROM tagged
