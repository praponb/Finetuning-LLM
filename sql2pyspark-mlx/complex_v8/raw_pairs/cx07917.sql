WITH valid_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LOWER(TRIM(event_type)) AS event_type_clean
    FROM ticket_events
    WHERE event_type = 'replied'
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        ticket_id,
        event_type,
        event_type_clean,
        LAST_VALUE(event_at) OVER (ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at,
        NTH_VALUE(event_at, 2) OVER (ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        ROW_NUMBER() OVER (ORDER BY event_at DESC, id) AS rn,
        RANK() OVER (PARTITION BY event_type ORDER BY event_at DESC) AS event_at_rank
    FROM valid_events
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT last_event_at, event_at_rank, event_type, rn
    FROM metrics
) src
PIVOT (
    SUM(rn)
    FOR event_type IN ('escalated', 'resolved', 'assigned', 'reopened')
)
ORDER BY last_event_at, event_at_rank
