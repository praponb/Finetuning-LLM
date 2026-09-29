WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE TO_DATE(event_at) < DATE '2025-03-14'
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LAST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY event_type, ticket_id ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY event_type, ticket_id ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        RANK() OVER w_event_type_ticket_id_by_even_2 AS event_at_rank
    FROM filtered
    WINDOW w_event_type_ticket_id_by_even_2 AS (PARTITION BY event_type, ticket_id ORDER BY event_at DESC)
),
joined_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.last_known_event_at,
        t.agent_id AS ticket_agent_id,
        su.agent_name
    FROM with_trends s
    LEFT JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        event_at,
        event_type,
        actor,
        ticket_agent_id,
        agent_name,
        DATEDIFF(DATE '2026-01-01', event_at) AS days_since_event_at,
        TO_DATE(event_at) AS event_at_date
    FROM joined_ticket
    WHERE MONTH(last_known_event_at) IN (1, 3)
),
derived AS (
    -- Add calculated columns
    SELECT *, ADD_MONTHS(event_at_date, 12) AS event_at_date_plus_12m
    FROM tagged
)
SELECT
    COALESCE(event_type, 'ALL') AS event_type,
    ticket_agent_id,
    COALESCE(actor, 'ALL') AS actor,
    ARRAY_SORT(COLLECT_SET(actor)) AS actor_list,
    SUM(days_since_event_at) AS total_days_since_event_at
FROM derived
GROUP BY GROUPING SETS ((event_type, ticket_agent_id, actor), (event_type), (ticket_agent_id), ())
