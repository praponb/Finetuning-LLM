WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LOWER(REGEXP_REPLACE(TRIM(event_type), ' ', '_')) AS event_type_slug,
        TO_DATE(event_at) AS event_at_date,
        DATEDIFF(DATE '2025-12-31', event_at) AS days_since_event_at
    FROM ticket_events
    WHERE (event_type NOT IN ('escalated', 'resolved') OR ticket_id >= 27)
),
enriched AS (
    -- Join tickets
    SELECT
        s.*,
        t.closed_at AS ticket_closed_at,
        t.csat AS ticket_csat,
        su.team AS agent_team
    FROM scoped s
    JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
with_activity AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.event_type IN ('escalated', 'resolved', 'assigned')
        )
)
SELECT id, field, metric_value
FROM (
    SELECT id, ticket_csat, days_since_event_at
    FROM with_activity
) src
UNPIVOT (
    metric_value FOR field IN (ticket_csat, days_since_event_at)
) unp
