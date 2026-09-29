WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATEDIFF(DATE '2026-01-01', event_at) AS days_since_event_at,
        DAYOFWEEK(event_at) AS event_at_dayofweek
    FROM ticket_events
    WHERE event_type = 'resolved'
),
joined_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.actor,
        s.days_since_event_at,
        s.event_at_dayofweek,
        t.requester_id AS ticket_requester_id
    FROM scoped s
    LEFT JOIN tickets t ON t.id = s.ticket_id AND t.requester_id > 11
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        ticket_id,
        actor,
        days_since_event_at,
        event_at_dayofweek,
        ticket_requester_id,
        ROUND(days_since_event_at / NULLIF(event_at_dayofweek, 0), 3) AS days_since_event_at_per_event_at_dayofweek,
        LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug,
        SUBSTRING(TRIM(actor), 1, 3) AS actor_prefix
    FROM joined_ticket
)
-- Subtotals with ROLLUP
SELECT
    COALESCE(actor, 'ALL') AS actor,
    ticket_id,
    PERCENTILE_APPROX(days_since_event_at_per_event_at_dayofweek, 0.5) AS median_days_since_event_at_per_event_at_dayofwee,
    ROUND(SUM(days_since_event_at_per_event_at_dayofweek), 2) AS total_days_since_event_at_per_event_at_dayofweek
FROM scored
GROUP BY ROLLUP(actor, ticket_id)
ORDER BY actor NULLS LAST, ticket_id NULLS LAST
