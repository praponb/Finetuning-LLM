WITH scoped AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATEDIFF(DATE '2026-01-01', event_at) AS days_since_event_at,
        LENGTH(TRIM(actor)) AS actor_len,
        LOWER(TRIM(actor)) AS actor_clean
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
      AND event_at >= TIMESTAMP '2025-01-13 14:00:00'
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        ticket_id,
        event_at,
        actor,
        days_since_event_at,
        actor_len,
        ROUND(days_since_event_at / NULLIF(SUM(days_since_event_at) OVER (PARTITION BY event_type, ticket_id), 0), 4) AS days_since_event_at_share_of_event_type_ticket_i,
        SUM(actor_len) OVER (PARTITION BY ticket_id, TO_DATE(event_at) ORDER BY days_since_event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_actor_len,
        ROUND(actor_len - AVG(actor_len) OVER w_event_type_ticket_id, 2) AS actor_len_vs_event_type_ticket_id_avg,
        COUNT(*) OVER (PARTITION BY event_type, ticket_id) AS rows_in_event_type_ticket_id,
        MAX(actor_len) OVER (PARTITION BY ticket_id, TO_DATE(event_at) ORDER BY days_since_event_at, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS actor_len_max_3
    FROM scoped
    WINDOW w_event_type_ticket_id AS (PARTITION BY event_type, ticket_id)
),
with_parent AS (
    -- Attach ticket attributes
    SELECT s.*, t.opened_at AS ticket_opened_at, t.priority AS ticket_priority
    FROM ranked s
    JOIN tickets t ON t.id = s.ticket_id
),
unique_rows AS (
    -- Keep the earliest row per ticket_id, actor
    SELECT
        id,
        ticket_id,
        event_at,
        actor,
        days_since_event_at,
        actor_len,
        days_since_event_at_share_of_event_type_ticket_i,
        running_actor_len,
        actor_len_vs_event_type_ticket_id_avg,
        rows_in_event_type_ticket_id,
        actor_len_max_3,
        ticket_opened_at,
        ticket_priority
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY ticket_id, actor ORDER BY rows_in_event_type_ticket_id, id) AS rn
        FROM with_parent
    ) ranked
    WHERE rn = 1
)
-- Final projection
SELECT
    id,
    actor,
    days_since_event_at,
    days_since_event_at_share_of_event_type_ticket_i,
    rows_in_event_type_ticket_id,
    actor_len_max_3,
    ticket_opened_at,
    ticket_priority,
    LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug,
    CASE WHEN actor_len_max_3 >= 36 THEN 'HIGH' WHEN actor_len_max_3 >= 14 THEN 'MEDIUM' ELSE 'LOW' END AS actor_len_max_3_tier
FROM unique_rows
ORDER BY actor, ticket_opened_at NULLS LAST, id
LIMIT 20
