WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LENGTH(TRIM(event_type)) AS event_type_len
    FROM ticket_events
)
SELECT event_type_len AS ref_id, ticket_id, event_at AS happened_at
FROM valid
UNION ALL
SELECT ticket_id, id, event_at
FROM ticket_events
WHERE event_type = 'assigned'
ORDER BY ticket_id DESC, ref_id
