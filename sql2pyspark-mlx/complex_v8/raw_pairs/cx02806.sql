WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown
    FROM ticket_events
    WHERE event_type NOT IN ('resolved', 'escalated')
      AND actor IS NOT NULL
),
with_activity AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.event_type IN ('assigned', 'reopened')
        )
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT_WS(': ', actor, event_type_or_unknown) AS actor_event_type_or_unknown_label,
        CASE WHEN actor IN ('customer', 'agent') THEN 'group_a' WHEN actor = 'system' THEN 'group_b' ELSE 'other' END AS actor_group
    FROM with_activity
    WHERE HOUR(event_at) BETWEEN 8 AND 13
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        actor,
        MAX(event_at) AS last_event_at,
        COUNT(*) AS n_rows,
        COUNT(*) FILTER (WHERE actor IS NOT NULL) AS n_actor_is_not_null
    FROM annotated
    WHERE ticket_id >= 44
    GROUP BY 1
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        FIRST_VALUE(last_event_at) IGNORE NULLS OVER (PARTITION BY n_rows ORDER BY n_actor_is_not_null ASC, actor ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_last_event_at,
        LAST_VALUE(n_actor_is_not_null) IGNORE NULLS OVER (PARTITION BY n_rows ORDER BY n_actor_is_not_null ASC, actor ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_n_actor_is_not_null,
        LAG(last_event_at) OVER (PARTITION BY n_rows ORDER BY n_actor_is_not_null ASC, actor) AS prev_last_event_at
    FROM stats
),
derived AS (
    -- Add calculated columns
    SELECT
        actor,
        n_rows,
        n_actor_is_not_null,
        first_known_last_event_at,
        prev_last_event_at,
        LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug,
        SUBSTRING(TRIM(actor), 1, 3) AS actor_prefix
    FROM windowed
)
-- Subtotals with GROUPING SETS
SELECT
    n_actor_is_not_null,
    COALESCE(actor, 'ALL') AS actor,
    GROUPING_ID() AS grouping_level,
    ROUND(STDDEV(n_actor_is_not_null), 2) AS stddev_n_actor_is_not_null
FROM derived
GROUP BY GROUPING SETS ((n_actor_is_not_null, actor), (n_actor_is_not_null), (actor), ())
