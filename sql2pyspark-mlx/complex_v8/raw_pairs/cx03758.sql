WITH filtered_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        UPPER(TRIM(actor)) AS actor_clean,
        LENGTH(TRIM(actor)) AS actor_len
    FROM ticket_events
    WHERE actor IN ('agent', 'customer')
),
with_activity AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM filtered_events s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.actor IN ('agent', 'system')
              AND t.event_type NOT IN ('reopened', 'created')
        )
),
annotated AS (
    -- Add calculated columns
    SELECT *, FLOOR(actor_len / 2) * 2 AS actor_len_bucket
    FROM with_activity
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS last_event_at,
        (SELECT COUNT(CASE WHEN t.actor IS NOT NULL THEN 1 END) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_actor_count
    FROM annotated s
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, value
FROM (
    SELECT id, actor_len_bucket, actor_len, event_actor_count
    FROM summarized_rows
) src
UNPIVOT (
    value FOR field IN (actor_len_bucket AS actor_len_bucket_v, actor_len AS m_actor_len, event_actor_count AS m_event_actor_count)
) unp
