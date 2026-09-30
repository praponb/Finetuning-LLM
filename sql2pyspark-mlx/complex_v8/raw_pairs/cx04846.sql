WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LOWER(REGEXP_REPLACE(TRIM(event_type), ' ', '_')) AS event_type_slug,
        CONCAT_WS('-', actor, event_type) AS actor_event_type_label
    FROM ticket_events
    WHERE MONTH(event_at) IN (1, 2)
      AND actor = 'customer'
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        ticket_id,
        actor,
        actor_event_type_label,
        SUBSTRING(TRIM(actor_event_type_label), 1, 4) AS actor_event_type_label_prefix,
        DATE_FORMAT(event_at, 'yyyy-MM-dd') AS event_at_label,
        UPPER(TRIM(actor)) AS actor_clean
    FROM scoped
    WHERE event_at >= TIMESTAMP '2025-01-25 09:00:00'
),
qualified AS (
    -- Keep rows whose ticket matches
    SELECT s.*
    FROM derived s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.id = s.ticket_id
              AND t.opened_at < TIMESTAMP '2025-02-24 10:00:00'
        )
),
with_details_ticket AS (
    -- Join tickets
    SELECT
        s.*,
        t.priority AS ticket_priority,
        t.closed_at AS ticket_closed_at,
        su.team AS agent_team
    FROM qualified s
    JOIN tickets t ON t.id = s.ticket_id
    JOIN support_agents su ON su.id = t.agent_id
    WHERE t.priority = 'P4'
),
derived_2 AS (
    -- Add calculated columns
    SELECT
        id,
        ticket_id,
        actor,
        actor_event_type_label,
        actor_event_type_label_prefix,
        event_at_label,
        ticket_priority,
        agent_team,
        SUBSTRING(TRIM(actor), 1, 4) AS actor_prefix,
        TO_DATE(ticket_closed_at) AS ticket_closed_at_date,
        DATE_TRUNC('MONTH', ticket_closed_at) AS ticket_closed_at_month
    FROM with_details_ticket
    WHERE ticket_id >= 17
),
rollup_base AS (
    -- Summarise per group
    SELECT
        actor,
        ticket_priority,
        COUNT(*) AS row_count,
        COUNT(DISTINCT ticket_id) AS distinct_ticket_ids
    FROM derived_2
    WHERE ticket_closed_at_date BETWEEN DATE '2024-09-01' AND DATE '2025-04-01'
    GROUP BY actor, ticket_priority
    HAVING COUNT(*) >= 2
)
-- Subtotals with GROUPING SETS
SELECT
    row_count,
    ticket_priority,
    actor,
    GROUPING_ID() AS grouping_level,
    SUM(distinct_ticket_ids) AS total_distinct_ticket_ids,
    ARRAY_SORT(COLLECT_SET(ticket_priority)) AS ticket_priority_list
FROM rollup_base
GROUP BY GROUPING SETS ((row_count, ticket_priority, actor), (row_count), (ticket_priority), ())
ORDER BY row_count NULLS LAST, ticket_priority NULLS LAST, actor NULLS LAST
