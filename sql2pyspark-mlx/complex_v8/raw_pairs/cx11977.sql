WITH filtered AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATEDIFF(DATE '2026-01-01', event_at) AS days_since_event_at,
        LENGTH(TRIM(event_type)) AS event_type_len,
        QUARTER(event_at) AS event_at_quarter
    FROM ticket_events
    WHERE actor = 'customer'
),
deduped AS (
    -- Keep the earliest row per actor, event_type
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        days_since_event_at,
        event_type_len,
        event_at_quarter
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY actor, event_type ORDER BY event_at, id) AS rn
        FROM filtered
    ) ranked
    WHERE rn = 1
),
combined_details_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        s.days_since_event_at,
        s.event_type_len,
        t.agent_id AS ticket_agent_id,
        su.hired_on AS agent_hired_on,
        su.tier AS agent_tier
    FROM deduped s
    JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
    WHERE t.agent_id <= 5
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(actor, 'ALL') AS actor,
    COALESCE(agent_tier, 'ALL') AS agent_tier,
    COALESCE(event_type, 'ALL') AS event_type,
    ROUND(STDDEV(days_since_event_at), 2) AS stddev_days_since_event_at,
    COUNT(*) AS n_rows,
    ROUND(STDDEV(event_type_len), 2) AS stddev_event_type_len
FROM combined_details_ticket
GROUP BY GROUPING SETS ((actor, agent_tier, event_type), (actor), (agent_tier), ())
ORDER BY actor NULLS LAST, agent_tier NULLS LAST, event_type NULLS LAST
