WITH cleaned AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LENGTH(TRIM(actor)) AS actor_len,
        TO_DATE(event_at) AS event_at_date,
        LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug
    FROM ticket_events
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(actor_len) IGNORE NULLS OVER (PARTITION BY actor ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_actor_len,
        MAX(actor_len) OVER (PARTITION BY actor ORDER BY event_at, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS actor_len_max_2_prior,
        ROUND(AVG(actor_len) OVER w_actor_len, 2) AS actor_len_avg_actor_len
    FROM cleaned
    WINDOW w_actor_len AS (PARTITION BY actor_len)
),
combined_details AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.actor_len,
        s.actor_slug,
        s.last_known_actor_len,
        s.actor_len_avg_actor_len,
        t.opened_at AS ticket_opened_at,
        t.requester_id AS ticket_requester_id
    FROM metrics s
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
screened AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM combined_details s
    WHERE s.ticket_id IN (
            SELECT t.ticket_id
            FROM ticket_events t
        )
),
combined_details_ticket AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.actor_len,
        s.actor_slug,
        s.actor_len_avg_actor_len,
        s.ticket_requester_id,
        t.opened_at AS ticket_opened_at_2,
        t.agent_id AS ticket_agent_id
    FROM screened s
    JOIN tickets t ON t.id = s.ticket_id
    WHERE t.agent_id >= 4
)
-- Final projection
SELECT
    id,
    ticket_id,
    actor_len,
    actor_slug,
    actor_len_avg_actor_len,
    ticket_requester_id,
    ticket_opened_at_2,
    ABS(ticket_requester_id - 9) AS ticket_requester_id_distance
FROM combined_details_ticket
ORDER BY ticket_opened_at_2, ticket_requester_id DESC
