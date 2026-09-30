WITH recent_tickets AS (
    -- Scope the tickets rows
    SELECT *
    FROM tickets
    WHERE (ARRAY_CONTAINS(tags, 'outage') OR MONTH(opened_at) IN (2, 3))
),
filtered AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM recent_tickets s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.id
              AND t.event_at < TIMESTAMP '2025-02-18 11:00:00'
        )
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        channel,
        csat,
        HOUR(opened_at) AS opened_at_hour
    FROM filtered
    WHERE agent_id <= 9
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT(channel, '#', CAST(csat AS STRING)) AS channel_ref,
        COALESCE(opened_at_hour, 0) AS opened_at_hour_filled
    FROM labeled
    WHERE channel IN ('web', 'email', 'chat')
)
SELECT id, measure, value
FROM (
    SELECT id, csat, opened_at_hour
    FROM tagged
) src
UNPIVOT INCLUDE NULLS (
    value FOR measure IN (csat, opened_at_hour)
) unp
ORDER BY id, measure
