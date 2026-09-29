WITH valid AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT_WS('-', event_type, actor) AS event_type_actor_label,
        CASE WHEN actor IN ('customer', 'system') THEN 'group_a' WHEN actor = 'agent' THEN 'group_b' ELSE 'other' END AS actor_group
    FROM ticket_events
    WHERE actor = 'system'
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LEAD(event_at) OVER (PARTITION BY actor, ticket_id ORDER BY event_at DESC, id) AS next_event_at,
        ROW_NUMBER() OVER (PARTITION BY actor, ticket_id ORDER BY event_at DESC, id) AS event_at_seq,
        NTILE(3) OVER (ORDER BY event_at, id) AS event_at_tercile,
        LAST_VALUE(event_at) IGNORE NULLS OVER (ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at
    FROM valid
)
SELECT id, metric_name, reading
FROM (
    SELECT id, event_at_tercile, event_at_seq
    FROM metrics
) src
UNPIVOT (
    reading FOR metric_name IN (event_at_tercile AS event_at_tercile_v, event_at_seq AS m_event_at_seq)
) unp
