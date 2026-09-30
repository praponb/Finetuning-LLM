WITH valid_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE actor = 'system'
      AND ticket_id IS NOT NULL
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        TO_DATE(event_at) AS event_at_date,
        CASE WHEN actor IN ('agent', 'system') THEN 'group_a' WHEN actor = 'customer' THEN 'group_b' ELSE 'other' END AS actor_group,
        LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug
    FROM valid_events
    WHERE actor = 'system'
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MIN(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS first_event_at,
        (SELECT MIN(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS first_event_at_2
    FROM tagged s
),
qualified AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM summarized_rows s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.event_type IN ('created', 'replied')
              AND t.actor NOT IN ('system', 'agent')
        )
      AND s.event_type IS NOT NULL
)
SELECT
    id,
    ticket_id,
    event_at,
    actor,
    actor_slug,
    LENGTH(TRIM(actor_slug)) AS actor_slug_len,
    LENGTH(TRIM(actor_group)) AS actor_group_len
FROM qualified
ORDER BY id
