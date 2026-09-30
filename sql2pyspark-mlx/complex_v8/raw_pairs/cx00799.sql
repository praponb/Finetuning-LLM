WITH valid AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_FORMAT(event_at, 'MMM yyyy') AS event_at_label
    FROM ticket_events
    WHERE actor = 'agent'
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        MAX(event_at) AS last_event_at,
        COUNT(*) AS event_count,
        COUNT(*) AS event_count_2
    FROM ticket_events
    GROUP BY ticket_id
),
with_stats AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.last_event_at,
        COALESCE(t.event_count, 0) AS event_count,
        COALESCE(t.event_count_2, 0) AS event_count_2
    FROM valid s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
),
annotated AS (
    -- Add calculated columns
    SELECT *, CONCAT_WS(': ', event_at_label, actor) AS event_at_label_actor_label
    FROM with_stats
    WHERE event_type IN ('assigned', 'reopened', 'resolved')
),
filtered AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM annotated s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
        )
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        event_at,
        event_type,
        actor,
        event_at_label,
        event_count,
        event_count_2,
        DATE_TRUNC('QUARTER', event_at) AS event_at_quarter,
        CONCAT_WS('|', actor, event_at_label) AS actor_event_at_label_label,
        CONCAT(event_type, '#', CAST(event_count AS STRING)) AS event_type_ref
    FROM filtered
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        actor,
        YEAR(event_at_quarter) AS event_at_quarter_year,
        TO_DATE(event_at) AS event_at_day,
        COUNT(*) FILTER (WHERE actor <> 'agent') AS n_actor_agent,
        ROUND(100 * COUNT(CASE WHEN actor NOT IN ('system', 'agent') THEN 1 END) / COUNT(*), 1) AS pct_actor_not_in_system_agent,
        SUM(CASE WHEN event_type <> 'created' THEN 1 ELSE 0 END) AS n_event_type_created,
        COUNT(*) AS row_count
    FROM tagged
    GROUP BY 1, 2, 3
)
SELECT
    actor,
    n_actor_agent,
    pct_actor_not_in_system_agent,
    n_event_type_created,
    row_count,
    CASE WHEN actor IN ('customer', 'system') THEN 'group_a' WHEN actor = 'agent' THEN 'group_b' ELSE 'other' END AS actor_group
FROM per_group
ORDER BY pct_actor_not_in_system_agent DESC NULLS FIRST, n_event_type_created
