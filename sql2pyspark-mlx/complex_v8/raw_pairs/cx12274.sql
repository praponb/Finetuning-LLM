WITH scoped_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LOWER(REGEXP_REPLACE(TRIM(event_type), ' ', '_')) AS event_type_slug
    FROM ticket_events
    WHERE MONTH(event_at) IN (1, 2)
),
screened AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM scoped_events s
    WHERE s.ticket_id IN (
            SELECT t.ticket_id
            FROM ticket_events t
            WHERE t.actor IN ('agent', 'customer')
        )
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT ticket_id, MAX(event_at) AS last_event_at
    FROM ticket_events
    GROUP BY ticket_id
),
profiled AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.last_event_at
    FROM screened s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
),
with_parent_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        t.priority AS ticket_priority,
        t.agent_id AS ticket_agent_id,
        su.tier AS agent_tier
    FROM profiled s
    JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
detail_rows_events AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.last_event_at,
        s.ticket_agent_id,
        t.id AS event_id,
        t.event_at AS event_event_at,
        t.actor AS event_actor,
        t.event_type AS event_event_type
    FROM with_parent_ticket s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id AND t.event_type = 'escalated'
),
grouped AS (
    -- Summarise per group
    SELECT
        event_actor,
        ARRAY_SORT(COLLECT_SET(event_event_type)) AS event_event_type_list,
        COUNT(DISTINCT event_id) AS distinct_event_ids
    FROM detail_rows_events
    WHERE event_id IS NOT NULL
    GROUP BY event_actor
),
tagged AS (
    -- Label and score rows
    SELECT
        event_actor,
        event_event_type_list,
        CONCAT(event_actor, '#', CAST(distinct_event_ids AS STRING)) AS event_actor_ref,
        COALESCE(distinct_event_ids, 0) AS distinct_event_ids_filled,
        COALESCE(distinct_event_ids, 0) AS distinct_event_ids_filled_2
    FROM grouped
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(event_actor, 'ALL') AS event_actor,
    distinct_event_ids_filled,
    GROUPING_ID() AS grouping_level,
    ROUND(SUM(distinct_event_ids_filled_2) / NULLIF(SUM(distinct_event_ids_filled), 0), 3) AS distinct_event_ids_filled_2_to_distinct_event_id,
    COUNT(*) FILTER (WHERE event_actor = 'system') AS n_event_actor_system,
    ROUND(SUM(distinct_event_ids_filled) / NULLIF(SUM(distinct_event_ids_filled_2), 0), 3) AS distinct_event_ids_filled_to_distinct_event_ids_
FROM tagged
GROUP BY GROUPING SETS ((event_actor, distinct_event_ids_filled), (event_actor), (distinct_event_ids_filled), ())
ORDER BY event_actor NULLS LAST, distinct_event_ids_filled NULLS LAST
