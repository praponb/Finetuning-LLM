WITH valid_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        UPPER(TRIM(event_type)) AS event_type_clean
    FROM ticket_events
    WHERE actor IS NOT NULL
),
grouped AS (
    -- Summarise per group
    SELECT
        event_type,
        SUM(CASE WHEN event_type = 'replied' THEN 1 ELSE 0 END) AS n_event_type_replied,
        SUM(CASE WHEN event_type IN ('resolved', 'created') THEN 1 ELSE 0 END) AS n_event_type_in_resolved_creat,
        COUNT(*) FILTER (WHERE actor = 'system') AS n_actor_system
    FROM valid_events
    GROUP BY event_type
),
filtered AS (
    -- Compare against the overall max
    SELECT *
    FROM grouped
    WHERE n_actor_system < (SELECT MAX(n_actor_system) FROM grouped)
),
scored AS (
    -- Add calculated columns
    SELECT
        event_type,
        n_event_type_replied,
        n_event_type_in_resolved_creat,
        n_actor_system,
        CASE WHEN n_actor_system >= 6 THEN 'A' WHEN n_actor_system >= 2 THEN 'B' ELSE 'C' END AS n_actor_system_tier
    FROM filtered
    WHERE n_actor_system BETWEEN 1 AND 7
)
-- Native SQL UNPIVOT (wide to long)
SELECT event_type, metric, metric_value
FROM (
    SELECT event_type, n_event_type_in_resolved_creat, n_actor_system, n_event_type_replied
    FROM scored
) src
UNPIVOT (
    metric_value FOR metric IN (n_event_type_in_resolved_creat, n_actor_system, n_event_type_replied)
) unp
ORDER BY event_type, metric
