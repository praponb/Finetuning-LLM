WITH scoped_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug,
        MONTH(event_at) AS event_at_month,
        INITCAP(TRIM(event_type)) AS event_type_clean
    FROM ticket_events
    WHERE ticket_id <= 3
      AND actor IN ('agent', 'customer')
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        t.agent_id AS ticket_agent_id,
        su.agent_name,
        su.hired_on AS agent_hired_on
    FROM scoped_events s
    LEFT JOIN tickets t ON t.id = s.ticket_id AND SIZE(t.tags) >= 1
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        event_at,
        event_type,
        actor,
        actor_slug,
        event_at_month,
        agent_hired_on,
        ROUND(CUME_DIST() OVER (ORDER BY event_at_month DESC), 3) AS event_at_month_cume_dist,
        ROUND(AVG(event_at_month) OVER (PARTITION BY actor, ticket_agent_id ORDER BY agent_hired_on DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS event_at_month_avg_6_prior
    FROM with_details
)
-- Final projection
SELECT
    id,
    event_at,
    event_type,
    actor_slug,
    event_at_month_cume_dist,
    event_at_month_avg_6_prior
FROM ranked
WHERE event_type IS NOT NULL
ORDER BY id, event_at
