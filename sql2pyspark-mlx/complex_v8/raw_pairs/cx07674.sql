WITH base AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        COALESCE(actor, 'UNKNOWN') AS actor_or_unknown
    FROM ticket_events
    WHERE event_type <> 'replied'
),
summary AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', event_at) AS event_at_month,
        event_type,
        actor_or_unknown,
        COUNT(*) FILTER (WHERE event_type NOT IN ('escalated', 'assigned')) AS n_event_type_not_in_escalated,
        COUNT(*) FILTER (WHERE event_type <> 'resolved') AS n_event_type_resolved,
        SUM(CASE WHEN event_type = 'assigned' THEN 1 ELSE 0 END) AS n_event_type_assigned,
        MIN(event_at) AS first_event_at,
        ARRAY_SORT(COLLECT_SET(event_type)) AS event_type_list
    FROM base
    WHERE actor IS NOT NULL
    GROUP BY DATE_TRUNC('MONTH', event_at), event_type, actor_or_unknown
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTILE(3) OVER (ORDER BY n_event_type_resolved DESC, event_at_month, event_type, actor_or_unknown) AS n_event_type_resolved_tercile,
        ROUND(n_event_type_resolved - AVG(n_event_type_resolved) OVER (PARTITION BY actor_or_unknown, n_event_type_not_in_escalated), 2) AS n_event_type_resolved_vs_actor_or_unknown_n_even,
        NTILE(3) OVER (PARTITION BY actor_or_unknown, n_event_type_assigned ORDER BY n_event_type_resolved, event_at_month, event_type, actor_or_unknown) AS n_event_type_resolved_tercile_2,
        NTH_VALUE(first_event_at, 2) OVER (PARTITION BY actor_or_unknown, n_event_type_assigned ORDER BY n_event_type_resolved, event_at_month, event_type, actor_or_unknown ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_first_event_at
    FROM summary
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM with_trends
    WHERE n_event_type_assigned > (SELECT AVG(n_event_type_assigned) FROM with_trends)
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY actor_or_unknown ORDER BY n_event_type_resolved_tercile_2), 3) AS n_event_type_resolved_tercile_2_percent_rank,
        ROUND(n_event_type_resolved - AVG(n_event_type_resolved) OVER (PARTITION BY event_type), 2) AS n_event_type_resolved_vs_event_type_avg
    FROM filtered
)
-- Final projection
SELECT
    event_at_month,
    actor_or_unknown,
    n_event_type_assigned,
    event_type_list,
    n_event_type_resolved_tercile,
    n_event_type_resolved_vs_actor_or_unknown_n_even,
    n_event_type_resolved_tercile_2,
    second_first_event_at,
    n_event_type_resolved_tercile_2_percent_rank,
    first_event_at + INTERVAL 7 DAYS AS first_event_at_plus_7d,
    SIZE(event_type_list) AS event_type_list_count
FROM metrics
ORDER BY n_event_type_resolved_tercile DESC NULLS FIRST
