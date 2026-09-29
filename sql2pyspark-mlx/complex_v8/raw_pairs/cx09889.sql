WITH base AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        UPPER(TRIM(event_type)) AS event_type_clean,
        DATE_TRUNC('QUARTER', event_at) AS event_at_quarter
    FROM ticket_events
    WHERE event_type NOT IN ('escalated', 'assigned')
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        ticket_id,
        actor,
        event_type_clean,
        COALESCE(actor, 'UNKNOWN') AS actor_or_unknown
    FROM base
),
combined_details_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.actor,
        s.event_type_clean,
        t.closed_at AS ticket_closed_at,
        t.csat AS ticket_csat,
        su.tier AS agent_tier
    FROM enriched s
    LEFT JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT ticket_closed_at, agent_tier, ticket_csat
    FROM combined_details_ticket
) src
PIVOT (
    MIN(ticket_csat)
    FOR agent_tier IN ('T2', 'T3', 'T1')
)
ORDER BY ticket_closed_at
