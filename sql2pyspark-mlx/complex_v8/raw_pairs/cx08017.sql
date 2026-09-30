WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LENGTH(TRIM(actor)) AS actor_len
    FROM ticket_events
    WHERE ticket_id >= 23
      AND event_type = 'replied'
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        actor,
        FLOOR(actor_len / 2) * 2 AS actor_len_bucket,
        INITCAP(TRIM(actor)) AS actor_clean
    FROM base
)
SELECT *
FROM (
    SELECT event_at, ticket_id, actor, actor_len_bucket
    FROM tagged
) src
PIVOT (
    MAX(actor_len_bucket)
    FOR actor IN ('system', 'agent')
)
